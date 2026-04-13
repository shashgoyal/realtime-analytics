"""
Event simulator — generates realistic traffic against the ingestion API.

Usage:
    python simulator.py                      # 10 eps, 50 users, runs forever
    python simulator.py --eps 100 --users 200 --duration 60
    python simulator.py --burst 500          # send 500 events as fast as possible
    
    
# Steady stream — 10 events/sec (default), runs until Ctrl+C
python simulator.py

# Higher throughput with more users
python simulator.py --eps 100 --users 200

# Timed run for demos — 50 eps for 30 seconds
python simulator.py --eps 50 --duration 30

# Burst / stress test — fire 500 events as fast as possible
python simulator.py --burst 500

# Custom thread count and API target
python simulator.py --eps 200 --workers 16 --url http://myhost:8000/event
"""

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
import requests

load_dotenv()

API_URL = os.getenv("API_URL", "http://localhost:8000/event")

EVENT_TYPES = ["click", "page_view", "scroll", "form_submit", "error", "logout", "signup", "purchase"]
PAGES = ["/home", "/products", "/products/1", "/products/2", "/cart", "/checkout",
         "/dashboard", "/settings", "/profile", "/about", "/blog", "/blog/1", "/search"]
DEVICES = ["mobile", "desktop", "tablet", "TV"]
DEVICE_WEIGHTS = [0.45, 0.35, 0.12, 0.08]
EVENT_WEIGHTS = [30, 25, 15, 8, 5, 4, 3, 10]


def make_user_pool(n: int) -> list[dict]:
    """Pre-generate a pool of users with sticky sessions and devices."""
    users = []
    for i in range(1, n + 1):
        users.append({
            "user_id": f"user-{i}",
            "session_id": uuid.uuid4().hex,
            "device": random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0],
            "ip_address": f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        })
    return users


def random_event(user: dict) -> dict:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    page = random.choice(PAGES)
    return {
        "user_id": user["user_id"],
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": user["session_id"],
        "page_url": page,
        "device": user["device"],
        "ip_address": user["ip_address"],
        "metadata": {"referrer": random.choice(["google", "direct", "twitter", "email", None])},
    }


def send_event(event: dict) -> bool:
    try:
        r = requests.post(API_URL, json=event, timeout=5)
        return r.status_code == 200
    except requests.RequestException:
        return False


def run_continuous(eps: float, users: list[dict], duration: float | None, workers: int):
    interval = 1.0 / eps if eps > 0 else 0
    sent = 0
    errors = 0
    start = time.monotonic()
    deadline = start + duration if duration else None

    print(f"Streaming at ~{eps} events/sec  |  {len(users)} users  |  workers={workers}")
    if duration:
        print(f"Will stop after {duration}s")
    print(f"Target: {API_URL}\n")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = []
        try:
            while True:
                if deadline and time.monotonic() >= deadline:
                    break

                event = random_event(random.choice(users))
                futures.append(pool.submit(send_event, event))

                # Drain completed futures periodically
                if len(futures) >= 200:
                    for f in as_completed(futures):
                        if f.result():
                            sent += 1
                        else:
                            errors += 1
                    futures.clear()

                elapsed = time.monotonic() - start
                expected = (sent + errors + len(futures)) * interval
                drift = expected - elapsed
                if drift > 0:
                    time.sleep(drift)

        except KeyboardInterrupt:
            print("\nStopping…")

        for f in as_completed(futures):
            if f.result():
                sent += 1
            else:
                errors += 1

    elapsed = time.monotonic() - start
    print(f"\nDone — {sent} sent, {errors} failed in {elapsed:.1f}s ({sent/elapsed:.1f} actual eps)")


def run_burst(count: int, users: list[dict], workers: int):
    print(f"Burst mode: sending {count} events as fast as possible  |  workers={workers}")
    print(f"Target: {API_URL}\n")

    start = time.monotonic()
    sent = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(send_event, random_event(random.choice(users)))
            for _ in range(count)
        ]
        for f in as_completed(futures):
            if f.result():
                sent += 1
            else:
                errors += 1

    elapsed = time.monotonic() - start
    print(f"\nDone — {sent} sent, {errors} failed in {elapsed:.1f}s ({sent/elapsed:.1f} eps)")


def main():
    parser = argparse.ArgumentParser(description="Event traffic simulator")
    parser.add_argument("--eps", type=float, default=10, help="Events per second (default: 10)")
    parser.add_argument("--users", type=int, default=50, help="Number of simulated users (default: 50)")
    parser.add_argument("--duration", type=float, default=None, help="Seconds to run (default: forever)")
    parser.add_argument("--burst", type=int, default=None, help="Send N events as fast as possible, then exit")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent HTTP threads (default: 8)")
    parser.add_argument("--url", type=str, default=None, help="Override API URL")
    args = parser.parse_args()

    if args.url:
        global API_URL
        API_URL = args.url

    users = make_user_pool(args.users)

    if args.burst:
        run_burst(args.burst, users, args.workers)
    else:
        run_continuous(args.eps, users, args.duration, args.workers)


if __name__ == "__main__":
    main()
