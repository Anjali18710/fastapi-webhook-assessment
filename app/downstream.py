import os

import httpx

DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "http://127.0.0.1:9001/events")


class RetryableDeliveryError(Exception):
    """Transient failure (5xx, 429, timeout, connection error). Safe to try again later."""


class PermanentDeliveryError(Exception):
    """The downstream rejected the event (4xx). Retrying the same payload will not help."""


def create_client() -> httpx.AsyncClient:
    """
    One shared client for the whole process, created at startup.
    Re-uses TCP connections (keep-alive) instead of opening a new client,
    connection pool and socket for every single event.
    """
    return httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=3.0),
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )


async def forward_event(
    client: httpx.AsyncClient,
    event_id: str,
    cleaned_text: str,
    fingerprint: str,
    idempotency_key: str,
    url: str | None = None,
) -> dict:
    """
    Forwards a processed webhook event to the downstream consumer API.

    Delivery is at-least-once, so the idempotency key is passed on as a header:
    a downstream that honours it can drop the rare redelivery itself.
    """
    try:
        response = await client.post(
            url or os.getenv("DOWNSTREAM_URL", DOWNSTREAM_URL),
            json={"event_id": event_id, "text": cleaned_text, "fingerprint": fingerprint},
            headers={"Idempotency-Key": idempotency_key},
        )
    except httpx.TransportError as exc:  # timeouts, refused connections, resets
        raise RetryableDeliveryError(f"{type(exc).__name__}: {exc}") from exc

    if response.status_code >= 500 or response.status_code == 429:
        raise RetryableDeliveryError(f"HTTP {response.status_code}")
    if response.status_code >= 400:
        raise PermanentDeliveryError(f"HTTP {response.status_code}: {response.text[:200]}")
    return response.json()
