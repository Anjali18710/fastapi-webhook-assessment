# Architecture & Debugging Decisions

## 1. Root-Cause Diagnosis

I ran the original code against the same 60-request burst as the test harness (30 events, each sent twice, concurrently):

| Metric | Original | Refactored |
|---|---|---|
| Max `/health` latency during burst | **~2,600 ms** | ~15–30 ms in my own probe (see note at the end of §2 about the official harness) |
| Ingest p95 | **~3,800 ms** | ~60–130 ms |
| HTTP 500s returned to sender | 5–6 of 60 | 0 |
| DB rows for 30 unique events | **60** | 30 |
| Downstream deliveries for 30 unique events | **~55** (≈25 duplicates) | 30 |
| Rows stuck in `pending` | every row whose delivery hit a 503 | 0 (all `delivered`) |

- **Event-loop / CPU behavior:**
  - `scrub_and_fingerprint()` runs 120,000 SHA-256 rounds (~36–60 ms) directly inside an `async def` handler. That blocks the event loop, so 60 requests queue up behind about 2–3 s of hashing. During that time nothing else runs, including `/health`.
  - Moving it to a thread would not help much. `hashlib` releases the GIL only for inputs over about 2 KB, and every round here hashes a 32-byte digest, so the thread would hold the GIL and the loop would still starve.
  - The handler also awaited the downstream call (250 ms of simulated latency, plus 503s) before replying. That tied the sender's acknowledgement time to the partner's latency.
- **Concurrency & race conditions:**
  - The `X-Idempotency-Key` header was only checked for presence. It was never stored, and `events` had no unique constraint, so every retry inserted a new row and triggered a new delivery.
  - A check-then-insert in Python ("SELECT, then INSERT if missing") would not fix this. There is an `await` between the two steps, so two identical requests can both see "missing" (a TOCTOU race).
  - A downstream 503 became `raise_for_status()`, then an unhandled exception, then a **500** to the sender. The sender then retries, which adds more duplicates (the feedback loop described in the brief).
  - Nothing ever retried the delivery itself, so those rows stayed `pending` forever.
  - `mark_delivered` matched on `event_id`, so it also updated rows that belonged to other deliveries.
- **Resource & connection management:**
  - A new `aiosqlite` connection, and therefore a new background thread, was opened for every DB call: three per request.
  - SQLite ran in its default rollback-journal mode with no `busy_timeout`. Writers block readers there, and a busy database fails immediately with "database is locked".
  - A new `httpx.AsyncClient` was created for every event. That means a new connection pool and a new TCP handshake each time, with no keep-alive reuse and no explicit timeouts.
  - Startup used the deprecated `@app.on_event`, and nothing was cleaned up on shutdown.

## 2. Concurrency & Offloading Strategy

The request path now does only cheap work. Everything slow or failure-prone happens after the acknowledgement:

| Stage | Where it runs | Why |
|---|---|---|
| Validate, regex PII scrub, `INSERT` | Request handler (event loop) | Regex on a size-capped payload (64 KB) takes microseconds. Scrubbing *before* persisting means raw PII never touches disk. |
| Return `202 Accepted` (or `200` for a duplicate) | Request handler | The row is committed first, so the ack is a durable promise. |
| 120k-round fingerprint | `ProcessPoolExecutor` (spawned processes) | True CPU parallelism outside the GIL. The event loop only awaits a future. |
| Downstream POST + retries | 16 async worker tasks, one shared `httpx.AsyncClient` | I/O-bound, so async is the right tool. Concurrency is bounded and connections are reused. |
| Retry scheduling, crash recovery | A sweeper task that polls SQLite | State lives in the DB, not in memory. |

**Why these primitives:**
- **`ProcessPoolExecutor` rather than `asyncio.to_thread`:** the hash holds the GIL (see §1), so a thread would still compete with the event loop. A process does not.
  - The pool uses the `spawn` start method, because forking a process that already runs threads (aiosqlite, uvloop) is unsafe.
  - The pool is warmed up at startup, so the first burst doesn't pay for process creation.
  - Each worker process drops itself to `SCHED_IDLE` on Linux (`nice 19` elsewhere). On a busy machine the OS then always schedules the API's event loop first, and hashing only uses spare CPU. Nobody is waiting on the hash with a timeout.
- **Not FastAPI `BackgroundTasks`:** they run in the same event loop after the response, so CPU work there would still block `/health`. They are also in-memory only: lost on a crash, with no retry, backoff or concurrency limit.
- **A SQLite outbox rather than a plain in-memory `asyncio.Queue`:** the queue exists only as a low-latency fast path. The ingest handler leases its own new row and pushes the id straight to the queue. The `events` table is the source of truth: `pending`, `attempts`, `next_attempt_at` and `lease_until` are all persisted. Retries don't need sleeping coroutines; a failed attempt just records `next_attempt_at`, and the sweeper picks the row up when it is due.
- **Retry policy:**
  - Exponential backoff (0.25 s × 2ⁿ, capped at 30 s) with jitter, so retries don't arrive in synchronized waves.
  - Retried: 5xx, 429, timeouts and connection errors.
  - Other 4xx responses go straight to `dead`, because sending the same payload again cannot succeed.
  - After 15 attempts the row moves to `dead` and is kept for inspection or replay.

**Note on the `/health < 50 ms` check in the official harness.** I developed this in a 2-vCPU Firecracker VM. There, the test client, the API, the mock downstream and the hash worker all share two cores. In that environment the harness's first `/health` probe takes about 48–76 ms. That probe goes out at the same instant as all 60 POSTs, each on a new TCP connection. To check whether this was my code, I temporarily replaced the ingest handler with one that does nothing (`return {}`). The first probe still took **59–77 ms** in the same environment. So the remaining latency comes from the HTTP stack and the client on two cores, not from this service blocking its loop. Server-side event-loop lag, measured with a 5 ms ticker during the burst, stayed at or below about 11 ms. All other assertions passed in 10 of 10 consecutive runs. On a machine with more cores the health check should have enough headroom to pass.

## 3. Idempotency & SQLite Concurrency

- **The database enforces idempotency, not Python.** `idempotency_key TEXT NOT NULL UNIQUE`, plus a single statement:
  ```sql
  INSERT ... ON CONFLICT(idempotency_key) DO NOTHING RETURNING id
  ```
  SQLite allows one writer at a time, so this statement is atomic. When two identical retries arrive in the same millisecond, exactly one gets an `id` back. Only that request enqueues delivery and returns `202`. The other gets no row, looks up the existing one and returns `200 {"status": "duplicate"}` without doing any work. This holds across connections and across processes; `tests/test_reliability.py` races 40 inserts over 4 separate connections.
- **Key reuse with a different payload** returns `409 Conflict`. The service stores a SHA-256 of the request body (`request_hash`) and compares it, instead of silently acknowledging a different event.
- **Exactly one deliverer per row:**
  - Workers claim rows with one atomic statement: `UPDATE ... SET lease_until=? WHERE id IN (SELECT ... due AND lease expired) RETURNING id`. Two workers, or two processes, cannot claim the same row.
  - The ingest handler inserts its row already leased, so the sweeper can't also pick it up.
- **SQLite settings:**
  - `journal_mode=WAL`: readers and the writer don't block each other, and commits are cheaper.
  - `synchronous=NORMAL`: in WAL mode this is safe against a *process* crash; only a power loss can lose the last commits.
  - `busy_timeout=5000`: wait for the write lock instead of failing.
  - Autocommit (`isolation_level=None`): every write is a single atomic statement, so each DB call is one trip to the aiosqlite thread.
  - The whole process shares one long-lived connection.
- **Downstream:** across a network the strongest guarantee you can get is at-least-once. Every delivery therefore carries an `Idempotency-Key` header, so the partner can drop the rare redelivery described in §4.

## 4. Failure Modes & Production Trade-offs

- **Downstream completely unreachable for 60 seconds:**
  - Ingestion is unaffected: webhooks are still stored and acknowledged in under 250 ms.
  - Each delivery attempt fails fast with a connection error and is rescheduled with backoff (≈0.25, 0.5, 1, 2, 4, 8, 16, 30 s…). Over 60 s each event uses roughly 8–10 of its 15 attempts, and nothing is lost. Rows simply wait in `pending`.
  - When the partner recovers, the sweeper drains the backlog with at most 16 concurrent requests. Jitter spreads the load out.
  - Trade-offs:
    - There is no circuit breaker, so during the outage each event still makes its own futile attempts.
    - A longer outage (roughly 3–5 minutes, depending on jitter) exhausts the attempts and moves rows to `dead`. Those rows then need a replay step, which is a single `UPDATE events SET status='pending', attempts=0 ...`.
- **Process crashes 50 ms after acknowledging:**
  - The row was committed *before* the `202`, so the event is not lost. The in-memory queue is lost, but it never held the real state.
  - The row is `pending` with a lease. After a restart, the sweeper reclaims it once the lease expires (60 s by default), recomputes the fingerprint if it was missing, and delivers it.
  - If the crash happened after the downstream accepted the event but before `delivered` was written, the event is sent a second time. This is the at-least-once window, and it is why the `Idempotency-Key` header is forwarded.
  - Trade-off: recovery can be delayed by up to the lease length. A shorter lease with a heartbeat, or clearing leases at startup when running single-instance, would shorten that delay.
- **`--workers 4` instead of 1:**
  - Correctness holds. Both the idempotency check (the UNIQUE constraint) and row claiming (the atomic `UPDATE ... RETURNING`) are enforced by SQLite across processes.
  - **The first thing to degrade is SQLite's single write lock.** Four ingest loops, 4 × 16 delivery workers and four sweepers polling every 200 ms would all queue for one lock. `busy_timeout` hides this at first, but ingest p95 climbs, and under a real spike writes eventually fail with "database is locked" once the 5 s timeout is exceeded.
  - Close behind:
    - **CPU oversubscription.** Each worker creates its own process pool, so a 4-core box would run 12 hashing processes plus 4 event loops.
    - **Per-process limits that multiply.** Downstream concurrency becomes 64, not 16, which could overwhelm the partner.
  - The fix I'd choose is to split roles: N stateless ingest workers plus **one** dedicated delivery process, or at that scale move to Postgres (`FOR UPDATE SKIP LOCKED`) and a real queue. The brief rules out that infrastructure here, so I did not add it.
