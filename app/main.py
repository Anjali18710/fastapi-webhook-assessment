from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.database import Database, request_hash
from app.downstream import create_client
from app.processor import scrub
from app.worker import DeliveryService

MAX_CONTENT_CHARS = 64_000


class WebhookPayload(BaseModel):
    event_id: str = Field(min_length=1, max_length=256)
    tenant_id: str = Field(min_length=1, max_length=256)
    # Bounded so that the inline regex scrub stays cheap.
    content: str = Field(max_length=MAX_CONTENT_CHARS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Database()
    await db.connect()
    client = create_client()
    delivery = DeliveryService(db, client)
    await delivery.start()

    app.state.db = db
    app.state.delivery = delivery
    try:
        yield
    finally:
        await delivery.stop()
        await client.aclose()
        await db.close()


app = FastAPI(title="Webhook Ingestion Service", lifespan=lifespan)


@app.get("/health")
async def health():
    # Deliberately trivial: it only proves the event loop is responsive.
    return {"status": "ok"}


@app.post("/webhooks/ingest", status_code=202)
async def ingest_webhook(
    payload: WebhookPayload,
    x_idempotency_key: str | None = Header(default=None, max_length=256),
):
    """
    Durably record the event and acknowledge it. Delivery happens in the background.

    202 Accepted  -> new event, stored and queued for delivery
    200 OK        -> duplicate retry of an event we already have (no new work done)
    409 Conflict  -> same idempotency key re-used with a *different* payload
    """
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing X-Idempotency-Key header")

    db: Database = app.state.db
    delivery: DeliveryService = app.state.delivery

    # Cheap regex redaction happens here, so raw PII never reaches disk.
    # The expensive fingerprint is computed later in a worker process.
    cleaned_text = scrub(payload.content)

    claim = delivery.lease_seconds if delivery.has_capacity() else None
    result = await db.insert_event(
        idempotency_key=x_idempotency_key,
        req_hash=request_hash(payload.event_id, payload.tenant_id, payload.content),
        event_id=payload.event_id,
        tenant_id=payload.tenant_id,
        cleaned_text=cleaned_text,
        claim_for_seconds=claim,
    )

    if not result.created:
        if result.conflict:
            raise HTTPException(
                status_code=409,
                detail="X-Idempotency-Key was already used with a different payload",
            )
        return JSONResponse(
            status_code=200,
            content={"status": "duplicate", "event_id": payload.event_id, "delivery_status": result.status},
        )

    if claim is None or not delivery.enqueue(result.row_id):
        # Queue is full: the row is safely stored, the sweeper will pick it up.
        if claim is not None:
            await db.release(result.row_id)
        delivery.wake()

    return {"status": "accepted", "event_id": payload.event_id}
