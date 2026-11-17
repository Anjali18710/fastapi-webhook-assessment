"""
Focused tests for the failure modes the burst harness does not exercise directly:
idempotency races across connections, key re-use with a different payload,
retry/dead-letter behaviour, downstream outages, and crash recovery.

These run in-process against a mocked downstream (httpx.MockTransport), so they are fast
and deterministic.
"""

import asyncio
import time

import httpx
import pytest

from app.database import Database, request_hash
from app.worker import DeliveryService


@pytest.fixture(autouse=True)
def fast_retries(monkeypatch):
    monkeypatch.setenv("FINGERPRINT_WORKERS", "1")
    monkeypatch.setenv("DELIVERY_BACKOFF_BASE", "0.02")
    monkeypatch.setenv("DELIVERY_BACKOFF_CAP", "0.1")
    monkeypatch.setenv("SWEEP_INTERVAL", "0.02")


@pytest.fixture
async def db(tmp_path):
    database = Database(str(tmp_path / "t.db"))
    await database.connect()
    yield database
    await database.close()


async def _insert(db: Database, key: str, content: str = "hello", claim: float | None = None):
    return await db.insert_event(key, request_hash("evt", "t1", content), "evt", "t1", content, claim)


async def _status(db: Database, key: str) -> tuple[str, int]:
    rows = await db.conn.execute_fetchall(
        "SELECT status, attempts FROM events WHERE idempotency_key = ?", (key,)
    )
    return rows[0]


async def _wait_for(predicate, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met in time")


# ------------------------------------------------------------------ idempotency


async def test_concurrent_duplicates_on_one_connection_create_one_row(db):
    results = await asyncio.gather(*(_insert(db, "same-key") for _ in range(50)))
    assert sum(r.created for r in results) == 1
    assert len({r.row_id for r in results}) == 1
    rows = await db.conn.execute_fetchall("SELECT COUNT(*) FROM events")
    assert rows[0][0] == 1


async def test_concurrent_duplicates_across_connections_create_one_row(tmp_path):
    """Simulates several processes (e.g. uvicorn --workers 4) sharing the same SQLite file."""
    path = str(tmp_path / "shared.db")
    conns = [Database(path) for _ in range(4)]
    for c in conns:
        await c.connect()
    try:
        results = await asyncio.gather(*(_insert(c, "k") for c in conns for _ in range(10)))
        assert sum(r.created for r in results) == 1
    finally:
        for c in conns:
            await c.close()


async def test_same_key_with_different_payload_is_a_conflict(db):
    first = await _insert(db, "k", content="original")
    second = await _insert(db, "k", content="tampered")
    assert first.created and not second.created
    assert second.conflict is True


async def test_two_workers_cannot_claim_the_same_row(db):
    await _insert(db, "k")  # unleased, due now
    claims = await asyncio.gather(*(db.claim_due(10, 30) for _ in range(5)))
    assert sum(len(c) for c in claims) == 1


# ------------------------------------------------------------------ delivery


def _service(db: Database, handler) -> tuple[DeliveryService, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return DeliveryService(db, client), client


async def test_transient_503s_are_retried_until_delivered(db):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.headers["Idempotency-Key"] == "k"
        return httpx.Response(503) if calls <= 2 else httpx.Response(200, json={"accepted": True})

    svc, client = _service(db, handler)
    await svc.start()
    try:
        await _insert(db, "k")
        await _wait_for(lambda: _status_is(db, "k", "delivered"))
        assert await _status(db, "k") == ("delivered", 3)
        assert calls == 3
    finally:
        await svc.stop()
        await client.aclose()


async def test_downstream_unreachable_then_recovers(db):
    """Connection refused for a while; the event stays pending and is delivered once it's back."""
    up_at = time.monotonic() + 0.5

    def handler(request: httpx.Request) -> httpx.Response:
        if time.monotonic() < up_at:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"accepted": True})

    svc, client = _service(db, handler)
    await svc.start()
    try:
        await _insert(db, "k")
        await _wait_for(lambda: _status_is(db, "k", "delivered"))
        _, attempts = await _status(db, "k")
        assert attempts > 1
    finally:
        await svc.stop()
        await client.aclose()


async def test_permanent_4xx_goes_to_dead_letter_without_retry(db):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(422, json={"detail": "bad"})

    svc, client = _service(db, handler)
    await svc.start()
    try:
        await _insert(db, "k")
        await _wait_for(lambda: _status_is(db, "k", "dead"))
        await asyncio.sleep(0.2)
        assert calls == 1
    finally:
        await svc.stop()
        await client.aclose()


async def test_retries_are_bounded(db, monkeypatch):
    monkeypatch.setenv("DELIVERY_MAX_ATTEMPTS", "3")
    svc, client = _service(db, lambda r: httpx.Response(503))
    await svc.start()
    try:
        await _insert(db, "k")
        await _wait_for(lambda: _status_is(db, "k", "dead"))
        assert await _status(db, "k") == ("dead", 3)
    finally:
        await svc.stop()
        await client.aclose()


async def test_event_acknowledged_before_a_crash_is_delivered_after_restart(db):
    """
    Simulates: row written + 202 returned, then the process died while holding the lease.
    A fresh DeliveryService must pick it up once the lease has expired.
    """
    await _insert(db, "k", claim=0.05)  # leased by the "crashed" process for 50ms
    delivered = []

    def handler(request: httpx.Request) -> httpx.Response:
        delivered.append(request)
        return httpx.Response(200, json={"accepted": True})

    svc, client = _service(db, handler)
    await svc.start()
    try:
        await _wait_for(lambda: _status_is(db, "k", "delivered"))
        assert len(delivered) == 1
        rows = await db.conn.execute_fetchall("SELECT fingerprint FROM events")
        assert rows[0][0] and len(rows[0][0]) == 64
    finally:
        await svc.stop()
        await client.aclose()


async def _status_is(db: Database, key: str, expected: str) -> bool:
    return (await _status(db, key))[0] == expected
