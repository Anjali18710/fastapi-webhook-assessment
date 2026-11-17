# Webhook Ingestion Service — Backend Technical Assessment

## Context
Our webhook ingestion service is dropping requests and timing out health checks during traffic spikes, and downstream partners are reporting duplicate events whenever their webhook sender retries after a slow response.

Your goal is to diagnose the root causes under realistic concurrent load and refactor the service so it handles traffic spikes reliably and idempotently.

## Setup
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## Running the Benchmark & Chaos Harness
The test suite starts both the FastAPI ingestion service and a mock flaky downstream receiver (`mock_downstream.py`), then simulates a 60-request burst containing duplicate retries while continuously probing `/health`:

```bash
pytest -v -s
```

*(Note: On the initial codebase, `pytest` is expected to fail.)*

## Acceptance Criteria
1. **Event-Loop Responsiveness:** Every `GET /health` probe sent while burst traffic is in flight must respond in **< 50ms**.
2. **Fast Ingestion Acknowledgment:** `POST /webhooks/ingest` must acknowledge valid requests (`200` or `202`) with **p95 latency < 250ms**.
3. **Strict Idempotency:** Concurrent duplicate deliveries sharing the same `X-Idempotency-Key` must never produce duplicate database rows or duplicate downstream deliveries.
4. **Resilient Downstream Delivery:** Transient `503` errors from the downstream service must be retried safely so all unique events reach `"delivered"` status.

## Constraints
- **No external infrastructure:** Keep the solution self-contained using Python and SQLite (do not add Redis, Celery, RabbitMQ, or PostgreSQL).
- Complete `DECISIONS.md` explaining your root-cause diagnosis and architectural choices.

---

## Solution Overview

See **[DECISIONS.md](DECISIONS.md)** for the full root-cause analysis and trade-offs.

```
POST /webhooks/ingest
  ├─ validate + regex PII scrub (cheap, inline: raw PII never hits disk)
  ├─ INSERT ... ON CONFLICT(idempotency_key) DO NOTHING RETURNING id   ← atomic idempotency
  └─ 202 Accepted   (200 = duplicate, 409 = key reused with a different payload)
          │
          ▼  (SQLite `events` table = durable outbox)
  delivery workers (16 async tasks, shared httpx client)
  ├─ fingerprint in ProcessPoolExecutor (spawned, low CPU priority)
  ├─ POST downstream with Idempotency-Key header
  └─ delivered | retry with exponential backoff + jitter | dead after 15 attempts
  sweeper: re-claims due retries, expired leases and rows left behind by a crash
```

| File | Responsibility |
|---|---|
| `app/main.py` | FastAPI app, lifespan wiring, ingest + health endpoints |
| `app/database.py` | SQLite schema (WAL), atomic idempotent insert, lease-based claiming |
| `app/worker.py` | Delivery workers, sweeper, retry/backoff, process pool |
| `app/downstream.py` | Shared HTTP client, retryable vs permanent error classification |
| `app/processor.py` | PII scrub and CPU-heavy fingerprint (split so they can run in different places) |
| `tests/test_reliability.py` | Extra tests: cross-connection races, 409 conflicts, retries, outage, dead-letter, crash recovery |

Tunables (environment variables): `DELIVERY_CONCURRENCY` (16), `FINGERPRINT_WORKERS` (cpu−1, max 4),
`DELIVERY_MAX_ATTEMPTS` (15), `DELIVERY_BACKOFF_BASE` (0.25 s), `DELIVERY_BACKOFF_CAP` (30 s),
`DELIVERY_LEASE_SECONDS` (60 s), `SWEEP_INTERVAL` (0.2 s).

> The schema changed. If you have a `webhooks.db` left over from the original code, delete it before starting the service.
