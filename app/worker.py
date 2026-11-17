"""
Background delivery pipeline.

    ingest handler ──(row already leased)──► asyncio.Queue ──► N delivery workers
                                                  ▲                 │
    sweeper (polls SQLite for due/expired rows) ──┘                 ├─ fingerprint in ProcessPoolExecutor
                                                                    ├─ POST downstream (shared httpx client)
                                                                    └─ mark delivered / schedule retry in SQLite

The queue is only a fast path. The source of truth is the `events` table:
anything not yet delivered is still `pending` there, so the sweeper will find it
again after a retry delay, an expired lease, or a process restart.
"""

import asyncio
import logging
import multiprocessing
import os
import random
from concurrent.futures import ProcessPoolExecutor

import httpx

from app.database import Database
from app.downstream import PermanentDeliveryError, RetryableDeliveryError, forward_event
from app.processor import fingerprint, init_worker_process, warmup

log = logging.getLogger("webhooks.worker")


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def default_cpu_workers() -> int:
    # Leave one core for the event loop (and everything else on the box).
    return max(1, min(4, (os.cpu_count() or 2) - 1))


class DeliveryService:
    def __init__(self, db: Database, client: httpx.AsyncClient):
        self.db = db
        self.client = client

        self.delivery_concurrency = _env_int("DELIVERY_CONCURRENCY", 16)
        self.cpu_workers = _env_int("FINGERPRINT_WORKERS", default_cpu_workers())
        self.max_attempts = _env_int("DELIVERY_MAX_ATTEMPTS", 15)
        self.backoff_base = _env_float("DELIVERY_BACKOFF_BASE", 0.25)  # seconds
        self.backoff_cap = _env_float("DELIVERY_BACKOFF_CAP", 30.0)  # seconds
        # Must comfortably exceed fingerprint time + HTTP timeout, or a slow attempt
        # could be picked up a second time by the sweeper.
        self.lease_seconds = _env_float("DELIVERY_LEASE_SECONDS", 60.0)
        self.sweep_interval = _env_float("SWEEP_INTERVAL", 0.2)

        self.queue: asyncio.Queue[int] = asyncio.Queue(maxsize=_env_int("QUEUE_MAXSIZE", 1000))
        self.pool: ProcessPoolExecutor | None = None
        self._tasks: list[asyncio.Task] = []
        self._wake = asyncio.Event()

    # ------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        # "spawn" gives clean child processes. Forking a process that already has
        # running threads (aiosqlite, the event loop) is unsafe.
        self.pool = ProcessPoolExecutor(
            max_workers=self.cpu_workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=init_worker_process,
        )
        # Start every worker process now, so the first burst doesn't pay process startup cost.
        loop = asyncio.get_running_loop()
        await asyncio.gather(*(loop.run_in_executor(self.pool, warmup) for _ in range(self.cpu_workers)))

        for i in range(self.delivery_concurrency):
            self._tasks.append(asyncio.create_task(self._worker_loop(), name=f"delivery-{i}"))
        self._tasks.append(asyncio.create_task(self._sweeper_loop(), name="sweeper"))
        log.info("delivery service started: %d workers, %d cpu procs", self.delivery_concurrency, self.cpu_workers)

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self.pool is not None:
            self.pool.shutdown(wait=True, cancel_futures=True)
            self.pool = None
        # Rows that were leased but not finished stay 'pending' in SQLite and are
        # reclaimed by the sweeper once their lease expires.

    # --------------------------------------------------------------- enqueue

    def has_capacity(self) -> bool:
        return not self.queue.full()

    def enqueue(self, row_id: int) -> bool:
        """Fast path from the ingest handler. Returns False if the local queue is full."""
        try:
            self.queue.put_nowait(row_id)
            return True
        except asyncio.QueueFull:
            return False

    def wake(self) -> None:
        self._wake.set()

    # ----------------------------------------------------------------- loops

    async def _sweeper_loop(self) -> None:
        """Finds due retries, expired leases and rows left over from a previous process."""
        while True:
            try:
                free = self.queue.maxsize - self.queue.qsize()
                for row_id in await self.db.claim_due(free, self.lease_seconds):
                    self.queue.put_nowait(row_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("sweeper iteration failed")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.sweep_interval)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def _worker_loop(self) -> None:
        while True:
            row_id = await self.queue.get()
            try:
                await self._process(row_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Never let one bad row kill the worker. The lease will expire and
                # the sweeper will retry it.
                log.exception("unexpected error processing row %s", row_id)
            finally:
                self.queue.task_done()

    # ------------------------------------------------------------ processing

    def _backoff(self, attempts_so_far: int) -> float:
        """Exponential backoff with jitter, capped. attempts_so_far >= 1."""
        delay = min(self.backoff_cap, self.backoff_base * (2 ** (attempts_so_far - 1)))
        return delay * random.uniform(0.5, 1.0)  # jitter avoids synchronized retry storms

    async def _process(self, row_id: int) -> None:
        event = await self.db.get_event(row_id)
        if event is None:  # already delivered or dead
            return

        if event.fingerprint is None:
            loop = asyncio.get_running_loop()
            fp = await loop.run_in_executor(self.pool, fingerprint, event.cleaned_text)
            await self.db.set_fingerprint(row_id, fp)
            event.fingerprint = fp

        try:
            await forward_event(
                self.client,
                event.event_id,
                event.cleaned_text,
                event.fingerprint,
                event.idempotency_key,
            )
        except RetryableDeliveryError as exc:
            delay = self._backoff(event.attempts + 1)
            status = await self.db.mark_retry(row_id, delay, str(exc), self.max_attempts)
            if status == "dead":
                log.error("event %s dead after %d attempts: %s", event.event_id, event.attempts + 1, exc)
            else:
                log.warning("event %s attempt %d failed (%s); retry in %.2fs",
                            event.event_id, event.attempts + 1, exc, delay)
            return
        except PermanentDeliveryError as exc:
            await self.db.mark_dead(row_id, str(exc))
            log.error("event %s permanently rejected: %s", event.event_id, exc)
            return

        await self.db.mark_delivered(row_id)
