from functools import lru_cache
from schema import Event
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import redis

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_redis = redis.Redis(host="localhost", port=6379, decode_responses=True)


@lru_cache(maxsize=1)
def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


@app.post("/event")
async def send_event(event: Event):
    try:
        producer = get_producer()
    except KafkaError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Cannot reach Kafka at localhost:9092 ({e}). Run: docker compose up -d",
        ) from e
    producer.send("events", value=event.model_dump())
    producer.flush(timeout=10)
    return {"status": "sent"}


@app.post("/events/batch")
async def send_events_batch(events: list[Event]):
    try:
        producer = get_producer()
    except KafkaError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Cannot reach Kafka at localhost:9092 ({e}). Run: docker compose up -d",
        ) from e
    for event in events:
        producer.send("events", value=event.model_dump())
    producer.flush(timeout=10)
    return {"status": "sent", "count": len(events)}


# ---------------------------------------------------------------------------
# Analytics endpoints
# ---------------------------------------------------------------------------

@app.get("/analytics/event-counts")
async def get_event_counts():
    """All-time count per event_type."""
    counts = _redis.hgetall("event_type_counts")
    return {k: int(v) for k, v in counts.items()}


@app.get("/analytics/unique-users")
async def get_unique_users():
    """Approximate unique users per 1-minute window (last hour)."""
    keys = _redis.keys("unique_users:*")
    result = {}
    for key in sorted(keys):
        val = _redis.get(key)
        if val is not None:
            window_start = key.replace("unique_users:", "")
            result[window_start] = int(val)
    return result


@app.get("/analytics/top-pages")
async def get_top_pages(limit: int = 10):
    """Top pages by hit count."""
    pages = _redis.zrevrange("top_pages", 0, limit - 1, withscores=True)
    return [{"page_url": page, "hits": int(score)} for page, score in pages]


@app.get("/analytics/devices")
async def get_device_breakdown():
    """All-time event count per device type."""
    counts = _redis.hgetall("device_counts")
    return {k: int(v) for k, v in counts.items()}


@app.get("/analytics/error-rate")
async def get_error_rate():
    """Error rate per 1-minute window (last hour)."""
    keys = _redis.keys("error_rate:*")
    result = []
    for key in sorted(keys):
        data = _redis.hgetall(key)
        if data:
            window_start = key.replace("error_rate:", "")
            result.append({
                "window": window_start,
                "total": int(data["total"]),
                "errors": int(data["errors"]),
                "rate": float(data["rate"]),
            })
    return result


@app.get("/analytics/events-per-user")
async def get_events_per_user(limit: int = 20):
    """Event count per user, sorted descending."""
    all_users = _redis.hgetall("events_per_user")
    sorted_users = sorted(all_users.items(), key=lambda x: int(x[1]), reverse=True)
    return [{"user_id": uid, "total": int(cnt)} for uid, cnt in sorted_users[:limit]]


@app.get("/analytics/summary")
async def get_summary():
    """Comprehensive dashboard overview pulling every available metric."""

    # --- Event counts by type ---
    event_counts = _redis.hgetall("event_type_counts")
    event_breakdown = {k: int(v) for k, v in event_counts.items()}
    total_events = sum(event_breakdown.values()) if event_breakdown else 0

    # --- Users ---
    all_users = _redis.hgetall("events_per_user")
    per_user_counts = [int(v) for v in all_users.values()] if all_users else []
    total_unique_users = len(per_user_counts)
    avg_events_per_user = round(sum(per_user_counts) / total_unique_users, 2) if total_unique_users else 0
    max_events_per_user = max(per_user_counts) if per_user_counts else 0

    sorted_users = sorted(all_users.items(), key=lambda x: int(x[1]), reverse=True)
    top_users = [{"user_id": uid, "total": int(cnt)} for uid, cnt in sorted_users[:5]]

    # --- Pages ---
    top_pages_raw = _redis.zrevrange("top_pages", 0, 4, withscores=True)
    top_pages = [{"page_url": p, "hits": int(s)} for p, s in top_pages_raw]
    total_page_hits = int(_redis.zcard("top_pages") or 0)

    # --- Devices ---
    device_counts = _redis.hgetall("device_counts")
    devices = {k: int(v) for k, v in device_counts.items()}
    device_total = sum(devices.values()) if devices else 0
    device_pct = {k: round(v / device_total * 100, 1) for k, v in devices.items()} if device_total else {}

    # --- Unique-users timeline (per-window) ---
    unique_keys = sorted(_redis.keys("unique_users:*"))
    unique_users_timeline = []
    latest_unique = 0
    for key in unique_keys:
        val = _redis.get(key)
        if val is not None:
            window_start = key.replace("unique_users:", "")
            count = int(val)
            unique_users_timeline.append({"window": window_start, "unique_users": count})
            latest_unique = count

    # --- Error-rate timeline ---
    error_keys = sorted(_redis.keys("error_rate:*"))
    error_timeline = []
    latest_error_rate = 0.0
    total_errors = 0
    for key in error_keys:
        data = _redis.hgetall(key)
        if data:
            window_start = key.replace("error_rate:", "")
            total = int(data.get("total", 0))
            errors = int(data.get("errors", 0))
            rate = float(data.get("rate", 0))
            total_errors += errors
            error_timeline.append({
                "window": window_start,
                "total": total,
                "errors": errors,
                "rate": rate,
            })
            latest_error_rate = rate

    # --- Windowed event counts (recent throughput timeline) ---
    windowed_keys = sorted(_redis.keys("event_counts:*:*"))
    throughput: dict[str, dict[str, int]] = {}
    for key in windowed_keys:
        parts = key.split(":", 2)
        if len(parts) == 3:
            _, etype, win = parts
            val = _redis.get(key)
            if val is not None:
                throughput.setdefault(win, {}).setdefault(etype, 0)
                throughput[win][etype] = int(val)

    throughput_timeline = [
        {"window": win, "by_type": counts, "total": sum(counts.values())}
        for win, counts in sorted(throughput.items())
    ]

    return {
        "total_events": total_events,
        "total_unique_users": total_unique_users,
        "total_unique_pages": total_page_hits,
        "total_errors": total_errors,
        "latest_unique_users": latest_unique,
        "latest_error_rate": latest_error_rate,
        "event_breakdown": event_breakdown,
        "devices": {
            "counts": devices,
            "percentages": device_pct,
        },
        "top_pages": top_pages,
        "top_users": top_users,
        "user_stats": {
            "avg_events_per_user": avg_events_per_user,
            "max_events_per_user": max_events_per_user,
        },
        "throughput_timeline": throughput_timeline,
        "unique_users_timeline": unique_users_timeline,
        "error_timeline": error_timeline,
    }
