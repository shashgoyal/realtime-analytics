import os

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

import pyspark
import redis
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, window, date_format, collect_set,
)
from pyspark.sql.types import StructType, StringType, TimestampType

_kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

REDIS_HOST = "localhost"
REDIS_PORT = 6379

EVENT_SCHEMA = (
    StructType()
    .add("user_id", StringType())
    .add("event_type", StringType())
    .add("timestamp", StringType())
    .add("session_id", StringType(), nullable=True)
    .add("page_url", StringType(), nullable=True)
    .add("device", StringType(), nullable=True)
    .add("ip_address", StringType(), nullable=True)
    .add("metadata", StringType(), nullable=True)
)

spark = (
    SparkSession.builder.appName("StreamingApp")
    .config("spark.jars.packages", _kafka_pkg)
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), EVENT_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("ts", col("timestamp").cast(TimestampType()))
)


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Aggregate each micro-batch in Spark, then write compact deltas to Redis."""
    if batch_df.isEmpty():
        return

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pipe = r.pipeline()

    # --- All-time aggregations (Spark groupBy → Redis incremental update) ---

    for row in batch_df.groupBy("event_type").count().collect():
        pipe.hincrby("event_type_counts", row["event_type"], row["count"])

    for row in batch_df.groupBy("user_id").count().collect():
        pipe.hincrby("events_per_user", row["user_id"], row["count"])

    for row in (
        batch_df.filter(col("page_url").isNotNull())
        .groupBy("page_url").count().collect()
    ):
        pipe.zincrby("top_pages", row["count"], row["page_url"])

    for row in (
        batch_df.filter(col("device").isNotNull())
        .groupBy("device").count().collect()
    ):
        pipe.hincrby("device_counts", row["device"], row["count"])

    # --- Windowed aggregations (1-minute tumbling window in Spark) ---

    ts_df = batch_df.filter(col("ts").isNotNull()).withColumn(
        "win_key",
        date_format(window(col("ts"), "1 minute")["start"], "yyyy-MM-dd'T'HH:mm:ss"),
    )

    windowed_counts = (
        ts_df.groupBy("win_key", "event_type").count().collect()
    )

    touched_windows: set[str] = set()
    for row in windowed_counts:
        wk, et, cnt = row["win_key"], row["event_type"], row["count"]

        pipe.incrby(f"event_counts:{et}:{wk}", cnt)
        pipe.expire(f"event_counts:{et}:{wk}", 3600)

        er_key = f"error_rate:{wk}"
        pipe.hincrby(er_key, "total", cnt)
        if et == "error":
            pipe.hincrby(er_key, "errors", cnt)
        pipe.expire(er_key, 3600)

        touched_windows.add(wk)

    # Unique users per window via HyperLogLog
    for row in (
        ts_df.groupBy("win_key")
        .agg(collect_set("user_id").alias("users"))
        .collect()
    ):
        hll_key = f"unique_users_hll:{row['win_key']}"
        pipe.pfadd(hll_key, *row["users"])
        pipe.expire(hll_key, 3600)

    pipe.execute()

    # Post-pipeline: materialise HLL counts and error rates for the API
    for wk in touched_windows:
        hll_key = f"unique_users_hll:{wk}"
        r.set(f"unique_users:{wk}", r.pfcount(hll_key), ex=3600)

        er_key = f"error_rate:{wk}"
        data = r.hgetall(er_key)
        if data:
            total = int(data.get("total", 0))
            errors = int(data.get("errors", 0))
            rate = round(errors / total, 4) if total > 0 else 0.0
            r.hset(er_key, mapping={"errors": errors, "rate": rate})

    r.close()


query = (
    parsed_df.writeStream.outputMode("append")
    .foreachBatch(process_batch)
    .queryName("all_analytics")
    .start()
)

spark.streams.awaitAnyTermination()
