# Realtime Analytics Pipeline

An end-to-end realtime analytics system that ingests user events via HTTP, streams them through Kafka, processes them with Spark Structured Streaming, stores aggregates in Redis, and visualizes everything in a live Next.js dashboard.

## Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌─────────┐     ┌──────────────────┐     ┌───────┐
│  Browser /   │────▶│  Ingestion API   │────▶│  Kafka  │────▶│  Spark Streaming │────▶│ Redis │
│  Simulator   │     │  (FastAPI)       │     │         │     │  (PySpark)       │     │       │
└──────────────┘     └──────────────────┘     └─────────┘     └──────────────────┘     └───┬───┘
                              ▲                                                            │
                              │                  ┌──────────────┐                          │
                              └──────────────────│  Dashboard   │◀─────────────────────────┘
                                 GET /analytics  │  (Next.js)   │  reads aggregates via API
                                                 └──────────────┘
```

**Write path:** Events are POSTed to the FastAPI ingestion service, validated against a Pydantic schema, and published to a Kafka topic.

**Compute path:** A PySpark Structured Streaming job consumes from Kafka in micro-batches, computes both all-time and 1-minute windowed aggregations, and writes results to Redis (hashes, sorted sets, HyperLogLog for unique user counts).

**Read path:** The FastAPI service also exposes analytics endpoints that read pre-aggregated data from Redis. The Next.js dashboard polls these endpoints and renders live charts and stats.

## Project Structure

```
├── docker-compose.yml       # Kafka (KRaft), Redis, Kafka UI
├── requirements.txt         # Python dependencies
├── schema.py                # Pydantic Event model
├── ingestion-service.py     # FastAPI — event ingestion + analytics API
├── simulator.py             # Load generator for synthetic traffic
├── spark-job/
│   └── streaming.py         # PySpark Structured Streaming job
└── dashboard/               # Next.js frontend
    └── src/app/
        ├── page.tsx         # Event Sender UI
        └── analytics/
            └── page.tsx     # Analytics Dashboard UI
```

## Tech Stack

| Layer       | Technology                                    |
| ----------- | --------------------------------------------- |
| Ingestion   | Python, FastAPI, kafka-python                 |
| Messaging   | Apache Kafka 3.9 (KRaft mode, single broker) |
| Processing  | PySpark Structured Streaming                  |
| Storage     | Redis 7 (hashes, sorted sets, HyperLogLog)   |
| Frontend    | Next.js 16, React 19, Tailwind CSS 4          |
| Tooling     | Docker Compose, OpenJDK 17                    |

## Prerequisites

- **Docker** and **Docker Compose**
- **Python 3.11+**
- **Java 17** (required by Spark) — on macOS: `brew install openjdk@17`
- **Node.js 18+** and **npm**

## Getting Started

### 1. Start infrastructure

```bash
docker compose up -d
```

This brings up Kafka on `localhost:9092`, Redis on `localhost:6379`, and the Kafka UI at `http://localhost:8080`.

### 2. Install Python dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Start the ingestion API

```bash
uvicorn ingestion-service:app --reload
```

The API will be available at `http://localhost:8000`. Key endpoints:

- `POST /event` — send a single event
- `POST /events/batch` — send a batch of events
- `GET /analytics/summary` — full dashboard payload
- `GET /analytics/event-counts`, `/analytics/unique-users`, `/analytics/top-pages`, `/analytics/devices`, `/analytics/error-rate`, `/analytics/events-per-user`

### 4. Start the Spark streaming job

> **Note:** If your Java installation is at a different path, update the `JAVA_HOME` line at the top of `spark-job/streaming.py`.

```bash
python spark-job/streaming.py
```

### 5. Start the dashboard

```bash
cd dashboard
npm install
npm run dev
```

Open `http://localhost:3000` for the Event Sender and `http://localhost:3000/analytics` for the live Analytics Dashboard.

### 6. Generate traffic (optional)

```bash
# Steady stream — 10 events/sec with 50 simulated users
python simulator.py

# Higher throughput
python simulator.py --eps 100 --users 200

# Timed run
python simulator.py --eps 50 --duration 30

# Burst mode — fire 500 events as fast as possible
python simulator.py --burst 500
```

## Event Schema

```json
{
  "user_id": "user-abc123",
  "event_type": "click",
  "timestamp": "2026-04-13T12:00:00Z",
  "session_id": "optional-session-id",
  "page_url": "/products",
  "device": "mobile",
  "ip_address": "192.168.1.1",
  "metadata": { "referrer": "google" }
}
```

Supported event types: `click`, `page_view`, `scroll`, `form_submit`, `error`, `logout`, `signup`, `purchase`.

## Analytics Computed

| Metric                   | Type             | Redis Structure |
| ------------------------ | ---------------- | --------------- |
| Event counts by type     | All-time         | Hash            |
| Events per user          | All-time         | Hash            |
| Top pages by hits        | All-time         | Sorted Set      |
| Device breakdown         | All-time         | Hash            |
| Unique users per window  | 1-min windowed   | HyperLogLog     |
| Error rate per window    | 1-min windowed   | Hash            |
| Throughput per event type| 1-min windowed   | String (counter)|
