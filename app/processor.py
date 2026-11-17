import hashlib
import re

PII_PATTERNS = [
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
]

DEFAULT_BLOB_ITERATIONS = 120_000


def scrub(raw_text: str) -> str:
    """
    Redacts common PII patterns.

    Cheap (microseconds for normal payloads, bounded by the request size limit),
    so it runs inline in the request handler. That way raw PII is never written
    to the database, not even temporarily.
    """
    cleaned = raw_text
    for pattern in PII_PATTERNS:
        cleaned = pattern.sub("[REDACTED]", cleaned)
    return cleaned


def fingerprint(cleaned_text: str, blob_iterations: int = DEFAULT_BLOB_ITERATIONS) -> str:
    """
    Computes a deterministic cryptographic fingerprint of already-scrubbed text.

    CPU-bound (~40-60ms per call). hashlib only releases the GIL for inputs larger
    than ~2KB, and every round here hashes a 32-byte digest, so this holds the GIL
    the whole time. It must run in a separate *process*, not a thread.
    Module-level and picklable so it can be sent to a ProcessPoolExecutor.
    """
    digest = cleaned_text.encode("utf-8")
    for _ in range(blob_iterations):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()


def scrub_and_fingerprint(raw_text: str, blob_iterations: int = DEFAULT_BLOB_ITERATIONS) -> tuple[str, str]:
    """Original combined API, kept for backwards compatibility."""
    cleaned = scrub(raw_text)
    return cleaned, fingerprint(cleaned, blob_iterations)


def init_worker_process() -> None:
    """
    Runs once in each worker process and drops it to background CPU priority, so that
    when cores are scarce the OS scheduler always prefers the API process's event loop
    (and its /health and ingest handlers) over hashing. Nobody is waiting on the hash
    with a timeout; it is fine for it to be the thing that gets slowed down.
    """
    import os

    try:
        # Linux: only run when a core would otherwise be idle.
        os.sched_setscheduler(0, os.SCHED_IDLE, os.sched_param(0))
        return
    except (AttributeError, OSError):
        pass
    try:
        os.nice(19)  # macOS / other Unix: lowest normal priority
    except (AttributeError, OSError):
        pass  # Windows: no os.nice; run at normal priority


def warmup() -> int:
    """No-op used to force worker processes to start before traffic arrives."""
    return 0
