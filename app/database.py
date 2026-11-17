"""
SQLite persistence layer.

The `events` table is both the idempotency ledger and a durable outbox:
a row is written *before* the webhook is acknowledged, and background workers
drive it from `pending` to `delivered` (or `dead`). Nothing about delivery state
lives only in memory, so a restart picks up exactly where the last process stopped.

A single long-lived aiosqlite connection is shared by the whole process.
aiosqlite runs every statement on one dedicated thread, so writes from this
process are serialized without "database is locked" errors, and the event loop
never blocks on disk I/O.
"""

import hashlib
import json
import os
import time
from dataclasses import dataclass

import aiosqlite

DB_PATH = os.getenv("WEBHOOK_DB_PATH", "webhooks.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key  TEXT    NOT NULL UNIQUE,
    request_hash     TEXT    NOT NULL,
    event_id         TEXT    NOT NULL,
    tenant_id        TEXT    NOT NULL,
    cleaned_text     TEXT    NOT NULL,
    fingerprint      TEXT,
    status           TEXT    NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'delivered', 'dead')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  REAL    NOT NULL,
    lease_until      REAL,
    last_error       TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    delivered_at     TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_events_due ON events (status, next_attempt_at);
"""


@dataclass
class InsertResult:
    created: bool
    row_id: int
    status: str
    conflict: bool = False  # same idempotency key, different payload


@dataclass
class EventRow:
    id: int
    idempotency_key: str
    event_id: str
    tenant_id: str
    cleaned_text: str
    fingerprint: str | None
    attempts: int


def request_hash(event_id: str, tenant_id: str, content: str) -> str:
    """Single SHA-256 of the request body. Cheap; used to detect key reuse with a different payload."""
    body = json.dumps([event_id, tenant_id, content], separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


class Database:
    def __init__(self, path: str | None = None):
        self.path = path or os.getenv("WEBHOOK_DB_PATH", DB_PATH)
        self._conn: aiosqlite.Connection | None = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected")
        return self._conn

    async def connect(self) -> None:
        # isolation_level=None -> autocommit: every statement is its own transaction.
        # Each of our writes is a single atomic statement, so no explicit BEGIN/COMMIT
        # is needed, and every DB call costs exactly one hop to the aiosqlite thread.
        self._conn = await aiosqlite.connect(self.path, isolation_level=None)
        # WAL: readers don't block the writer and vice versa; much cheaper commits.
        await self._conn.execute("PRAGMA journal_mode=WAL")
        # In WAL mode NORMAL is still crash-safe for the database file; at worst the
        # last few commits are lost on *power* loss (not on a process crash).
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        # If another process holds the write lock, wait instead of failing immediately.
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.executescript(SCHEMA)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------ ingest

    async def insert_event(
        self,
        idempotency_key: str,
        req_hash: str,
        event_id: str,
        tenant_id: str,
        cleaned_text: str,
        claim_for_seconds: float | None,
    ) -> InsertResult:
        """
        Atomically record a new event, or report that the key already exists.

        Idempotency is enforced by the UNIQUE constraint on idempotency_key, not by a
        read-then-write check in Python. `INSERT ... ON CONFLICT DO NOTHING RETURNING id`
        is a single statement executed under SQLite's write lock, so when two identical
        retries race, exactly one gets a row id back and the other gets nothing.

        If claim_for_seconds is set, the new row is leased to the caller immediately,
        so the background sweeper will not pick it up as well.
        """
        now = time.time()
        lease = now + claim_for_seconds if claim_for_seconds else None
        rows = await self.conn.execute_fetchall(
            """
            INSERT INTO events (idempotency_key, request_hash, event_id, tenant_id,
                                cleaned_text, status, next_attempt_at, lease_until)
            VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
            ON CONFLICT(idempotency_key) DO NOTHING
            RETURNING id
            """,
            (idempotency_key, req_hash, event_id, tenant_id, cleaned_text, now, lease),
        )
        row = rows[0] if rows else None

        if row is not None:
            return InsertResult(created=True, row_id=row[0], status="pending")

        rows = await self.conn.execute_fetchall(
            "SELECT id, status, request_hash FROM events WHERE idempotency_key = ?",
            (idempotency_key,),
        )
        existing = rows[0]
        return InsertResult(
            created=False,
            row_id=existing[0],
            status=existing[1],
            conflict=existing[2] != req_hash,
        )

    # ---------------------------------------------------------------- outbox

    async def claim_due(self, limit: int, lease_seconds: float) -> list[int]:
        """
        Atomically lease up to `limit` pending rows whose retry time has come and whose
        previous lease (if any) has expired. One UPDATE statement, so two workers (or two
        processes) can never claim the same row.
        """
        if limit <= 0:
            return []
        now = time.time()
        rows = await self.conn.execute_fetchall(
            """
            UPDATE events SET lease_until = ?
            WHERE id IN (
                SELECT id FROM events
                WHERE status = 'pending'
                  AND next_attempt_at <= ?
                  AND (lease_until IS NULL OR lease_until < ?)
                ORDER BY next_attempt_at
                LIMIT ?
            )
            RETURNING id
            """,
            (now + lease_seconds, now, now, limit),
        )
        return [r[0] for r in rows]

    async def release(self, row_id: int) -> None:
        """Give a lease back without counting an attempt (e.g. local queue was full)."""
        await self.conn.execute("UPDATE events SET lease_until = NULL WHERE id = ?", (row_id,))

    async def get_event(self, row_id: int) -> EventRow | None:
        rows = await self.conn.execute_fetchall(
            """
            SELECT id, idempotency_key, event_id, tenant_id, cleaned_text, fingerprint, attempts
            FROM events WHERE id = ? AND status = 'pending'
            """,
            (row_id,),
        )
        row = rows[0] if rows else None
        return EventRow(*row) if row else None

    async def set_fingerprint(self, row_id: int, fp: str) -> None:
        await self.conn.execute("UPDATE events SET fingerprint = ? WHERE id = ?", (fp, row_id))

    async def mark_delivered(self, row_id: int) -> None:
        await self.conn.execute(
            """
            UPDATE events
            SET status = 'delivered', attempts = attempts + 1, lease_until = NULL,
                last_error = NULL, delivered_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (row_id,),
        )

    async def mark_retry(self, row_id: int, delay_seconds: float, error: str, max_attempts: int) -> str:
        """Record a failed attempt; schedule a retry, or move to 'dead' once attempts are exhausted."""
        rows = await self.conn.execute_fetchall(
            """
            UPDATE events
            SET attempts = attempts + 1,
                status = CASE WHEN attempts + 1 >= ? THEN 'dead' ELSE 'pending' END,
                next_attempt_at = ?,
                lease_until = NULL,
                last_error = ?
            WHERE id = ?
            RETURNING status
            """,
            (max_attempts, time.time() + delay_seconds, error[:500], row_id),
        )
        row = rows[0] if rows else None
        return row[0] if row else "unknown"

    async def mark_dead(self, row_id: int, error: str) -> None:
        await self.conn.execute(
            """
            UPDATE events SET status = 'dead', attempts = attempts + 1,
                              lease_until = NULL, last_error = ?
            WHERE id = ?
            """,
            (error[:500], row_id),
        )


# --- Backwards-compatible helper kept for anything that imported it directly ---

async def init_db(db_path: str | None = None):
    db = Database(db_path)
    await db.connect()
    await db.close()
