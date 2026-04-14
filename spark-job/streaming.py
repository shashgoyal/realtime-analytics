import os
import re

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import logging

import pyspark
import redis
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, window, date_format, collect_set, to_date,
)
from pyspark.sql.types import StructType, StringType, TimestampType

log = logging.getLogger(__name__)

_kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
_hadoop_aws_pkg = "org.apache.hadoop:hadoop-aws:3.4.2"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "events")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
S3_ARCHIVE_BUCKET = os.getenv("S3_ARCHIVE_BUCKET", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DYNAMODB_SNAPSHOTS_TABLE = os.getenv("DYNAMODB_SNAPSHOTS_TABLE", "")

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

_packages = [_kafka_pkg]
if S3_ARCHIVE_BUCKET:
    _packages.append(_hadoop_aws_pkg)

_builder = (
    SparkSession.builder.appName("StreamingApp")
    .config("spark.jars.packages", ",".join(_packages))
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
)

if S3_ARCHIVE_BUCKET:
    _builder = (
        _builder
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
    )

spark = _builder.getOrCreate()

topic_pattern = re.escape(KAFKA_TOPIC_PREFIX) + r"\..*"

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribePattern", topic_pattern)
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), EVENT_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("ts", col("timestamp").cast(TimestampType()))
)


def _archive_to_s3(batch_df: DataFrame, batch_id: int) -> None:
    """Write raw events to S3 as Parquet, partitioned by event_type and date."""
    archive_df = batch_df.withColumn("date", to_date(col("ts")))
    archive_df.write.mode("append").partitionBy("event_type", "date").parquet(
        f"s3a://{S3_ARCHIVE_BUCKET}/raw-events/"
    )
    log.info("Batch %d: archived %d events to s3://%s/raw-events/",
             batch_id, batch_df.count(), S3_ARCHIVE_BUCKET)


def _snapshot_to_dynamodb(r: redis.Redis, batch_id: int) -> None:
    """Write a point-in-time snapshot of current Redis aggregations to DynamoDB."""
    from datetime import datetime, timezone, timedelta
    from decimal import Decimal

    import boto3

    now = datetime.now(timezone.utc)
    ttl = int((now + timedelta(days=7)).timestamp())

    event_counts = r.hgetall("event_type_counts")
    event_breakdown = {k: int(v) for k, v in event_counts.items()}
    total_events = sum(event_breakdown.values()) if event_breakdown else 0

    device_raw = r.hgetall("device_counts")
    device_counts = {k: int(v) for k, v in device_raw.items()}

    all_users = r.hgetall("events_per_user")
    total_unique_users = len(all_users)

    latest_error_rate = Decimal("0")
    total_errors = 0
    error_keys = r.keys("error_rate:*")
    for key in sorted(error_keys):
        data = r.hgetall(key)
        if data:
            total_errors += int(data.get("errors", 0))
            latest_error_rate = Decimal(str(data.get("rate", 0)))

    def _to_dynamo(obj: dict) -> dict:
        """Convert int values to Decimal for DynamoDB compatibility."""
        return {k: Decimal(str(v)) for k, v in obj.items()}

    table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMODB_SNAPSHOTS_TABLE)
    table.put_item(Item={
        "pk": "snapshot",
        "sk": now.replace(microsecond=0).isoformat(),
        "total_events": total_events,
        "total_unique_users": total_unique_users,
        "total_errors": total_errors,
        "error_rate": latest_error_rate,
        "event_breakdown": _to_dynamo(event_breakdown),
        "device_counts": _to_dynamo(device_counts),
        "ttl": ttl,
    })
    log.info("Batch %d: snapshot written to DynamoDB table %s",
             batch_id, DYNAMODB_SNAPSHOTS_TABLE)


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Aggregate each micro-batch in Spark, then write compact deltas to Redis."""
    if batch_df.isEmpty():
        return

    if S3_ARCHIVE_BUCKET:
        try:
            _archive_to_s3(batch_df, batch_id)
        except Exception:
            log.exception("Batch %d: S3 archive failed, continuing with Redis", batch_id)

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

    # --- Cross-dimensional aggregations for filtered analytics ---

    has_page = col("page_url").isNotNull()
    has_device = col("device").isNotNull()

    for row in (
        batch_df.filter(has_page)
        .groupBy("page_url", "event_type").count().collect()
    ):
        pipe.hincrby(f"page_events:{row['page_url']}", row["event_type"], row["count"])

    for row in (
        batch_df.filter(has_page & has_device)
        .groupBy("page_url", "device").count().collect()
    ):
        pipe.hincrby(f"page_devices:{row['page_url']}", row["device"], row["count"])

    for row in batch_df.groupBy("user_id", "event_type").count().collect():
        pipe.hincrby(f"user_events:{row['user_id']}", row["event_type"], row["count"])

    for row in (
        batch_df.filter(has_device)
        .groupBy("user_id", "device").count().collect()
    ):
        pipe.hincrby(f"user_devices:{row['user_id']}", row["device"], row["count"])

    for row in (
        batch_df.filter(has_page)
        .groupBy("user_id", "page_url").count().collect()
    ):
        pipe.hincrby(f"user_pages:{row['user_id']}", row["page_url"], row["count"])

    for row in (
        batch_df.filter(has_device)
        .groupBy("device", "event_type").count().collect()
    ):
        pipe.hincrby(f"device_events:{row['device']}", row["event_type"], row["count"])

    for row in (
        batch_df.filter(has_device & has_page)
        .groupBy("device", "page_url").count().collect()
    ):
        pipe.hincrby(f"device_pages:{row['device']}", row["page_url"], row["count"])

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

    if DYNAMODB_SNAPSHOTS_TABLE:
        try:
            _snapshot_to_dynamodb(r, batch_id)
        except Exception:
            log.exception("Batch %d: DynamoDB snapshot failed, continuing", batch_id)

    r.close()


query = (
    parsed_df.writeStream.outputMode("append")
    .foreachBatch(process_batch)
    .queryName("all_analytics")
    .start()
)

spark.streams.awaitAnyTermination()
