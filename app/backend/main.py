"""
FastAPI backend for the Fraud Analytics Demonstrator.

Serves the React frontend as static files and provides REST + SSE endpoints
for real-time transaction data.

Two operational modes (auto-detected from config):
  - Kafka mode: generator publishes raw txns to Kafka; a separate Spark RTM job
    scores them and writes to Lakebase; a background poller here reads new scored
    rows from Lakebase and pushes them to SSE subscribers.
  - Inline mode (no Kafka): generator scores in-process via Model Serving and
    a bridge task inserts into Lakebase. SSE fed from in-memory queues.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from . import config
from .db import get_db, shutdown_db
from .generator import EventGenerator

STATIC_DIR = Path(__file__).parent.parent / "frontend" / "dist"

generator = EventGenerator()


# ---------------------------------------------------------------------------
# Inline mode: bridge generator queue -> Lakebase
# ---------------------------------------------------------------------------

async def _bridge_generator_to_db():
    """Background task: moves scored transactions from generator queue into Lakebase.

    Per-insert exceptions are logged and swallowed so that a transient DB error
    (e.g. a stale pooled connection after a long idle period between Stop/Start)
    cannot kill the bridge task permanently. The DB call is offloaded to a
    thread so the synchronous psycopg2 work does not block the event loop.

    Throughput is logged every 25 inserts so it is obvious from `/logz` whether
    the pipeline is flowing end-to-end.
    """
    db = get_db()
    q = generator.subscribe()
    log.info("Bridge task started -- db backend=%s, subscribed queue id=%s",
             db.__class__.__name__, id(q))
    ok = 0
    failed = 0
    try:
        while True:
            txn = await q.get()
            try:
                await asyncio.to_thread(db.insert_transaction, txn)
                ok += 1
                if ok == 1 or ok % 25 == 0:
                    log.info("Bridge inserted %d txns so far (last id=%s), %d failures",
                             ok, txn.get("id"), failed)
            except Exception:
                failed += 1
                log.exception("Bridge insert failed for txn id=%s", txn.get("id"))
    except asyncio.CancelledError:
        log.info("Bridge task stopping (ok=%d, failed=%d)", ok, failed)
        generator.unsubscribe(q)


# ---------------------------------------------------------------------------
# Kafka mode: poll Lakebase for new scored rows -> push to SSE subscribers
# ---------------------------------------------------------------------------

async def _poll_lakebase_for_sse():
    """Background task: polls Lakebase every ~1.5s for new scored transactions
    written by the Spark streaming job, and injects them into the generator's
    subscriber queues so the SSE endpoint picks them up."""
    db = get_db()
    last_seen = datetime.now(timezone.utc).isoformat()
    poll_interval = 1.5

    log.info("Lakebase poller started (Kafka mode) -- polling every %.1fs", poll_interval)
    try:
        while True:
            await asyncio.sleep(poll_interval)
            try:
                new_rows = await asyncio.to_thread(db.get_transactions_since, last_seen)
                for txn in new_rows:
                    await generator.publish_external(txn)
                if new_rows:
                    last_seen = new_rows[-1].get("scored_at", last_seen)
            except Exception:
                log.exception("Lakebase poller error")
    except asyncio.CancelledError:
        log.info("Lakebase poller stopped")


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    db = get_db()

    # Seed the generator's id counter past any existing rows in the DB so we
    # don't replay IDs and silently hit `ON CONFLICT (id) DO NOTHING` on every
    # restart.
    try:
        from .generator import bootstrap_id_counter
        max_id = await asyncio.to_thread(db.get_max_id)
        next_id = bootstrap_id_counter(max_id)
        log.info("Generator id counter bootstrapped: max_existing_id=%s, next_id=%s",
                 max_id, next_id)
    except Exception:
        log.exception("Failed to bootstrap id counter; continuing with default start")

    if generator.kafka_mode:
        log.info("Starting in KAFKA mode -- Lakebase poller active, no inline bridge")
        bg_task = asyncio.create_task(_poll_lakebase_for_sse())
    else:
        log.info("Starting in INLINE mode -- bridge task active")
        bg_task = asyncio.create_task(_bridge_generator_to_db())

    yield

    bg_task.cancel()
    generator.stop()
    if generator.kafka_mode:
        from . import kafka_producer
        kafka_producer.close()
    shutdown_db()


app = FastAPI(title="Fraud Analytics Demonstrator", lifespan=lifespan)


# --- SSE endpoint ---

async def _event_stream(request: Request) -> AsyncGenerator[dict, None]:
    q = generator.subscribe()
    try:
        while True:
            if await request.is_disconnected():
                break
            try:
                txn = await asyncio.wait_for(q.get(), timeout=2.0)
                yield {"event": "transaction", "data": json.dumps(txn)}
            except asyncio.TimeoutError:
                yield {"event": "ping", "data": ""}
    finally:
        generator.unsubscribe(q)


@app.get("/api/events")
async def sse_events(request: Request):
    return EventSourceResponse(_event_stream(request))


# --- REST endpoints ---

@app.get("/api/metrics")
async def get_metrics():
    return get_db().get_metrics()


@app.get("/api/transactions")
async def get_transactions(limit: int = Query(default=50, le=200)):
    return get_db().get_latest_transactions(limit)


@app.get("/api/country-flows")
async def get_country_flows():
    return get_db().get_country_flows()


@app.get("/api/time-series")
async def get_time_series():
    return get_db().get_time_series()


# --- Generator control ---

class SpeedRequest(BaseModel):
    tps: float


@app.post("/api/generator/start")
async def generator_start():
    generator.start()
    return generator.status()


@app.post("/api/generator/stop")
async def generator_stop():
    generator.stop()
    return generator.status()


@app.post("/api/generator/speed")
async def generator_speed(body: SpeedRequest):
    generator.set_speed(body.tps)
    return generator.status()


@app.get("/api/generator/status")
async def generator_status():
    return generator.status()


# --- DB health / identity ---

@app.get("/api/db-status")
async def db_status():
    """Reports which DB backend is active and whether it is reachable.

    Used by the UI to surface a positive confirmation that the deployed app is
    actually writing to Lakebase (vs having fallen back to the in-memory
    MockDB).
    """
    db = get_db()
    backend = "lakebase" if db.__class__.__name__ == "LakebaseDB" else "mock"
    connected = await asyncio.to_thread(db.ping)
    host = config.LAKEBASE_HOST if backend == "lakebase" else None
    database = config.LAKEBASE_DATABASE if backend == "lakebase" else None
    return {
        "backend": backend,
        "connected": connected,
        "host": host,
        "database": database,
    }


@app.post("/api/reset")
async def reset_data():
    from .generator import reset_id_counter
    generator.stop()
    generator.total_generated = 0
    reset_id_counter()
    get_db().clear_data()
    return {"status": "cleared"}


# --- Fraud explanation agent proxy ---

class ChatMessage(BaseModel):
    role: str  # "user" | "assistant" | "system"
    content: str


class ExplainRequest(BaseModel):
    messages: list[ChatMessage]


@app.post("/api/explain-fraud")
async def explain_fraud(body: ExplainRequest):
    """Proxies the Operations tab chat UI to the `bank-fraud-explain` agent.

    Keeps the workspace bearer token server-side (the SDK resolves it from
    the app SP). The blocking SDK call is offloaded to a thread so the
    event loop is not stalled while the agent thinks.
    """
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        payload = [{"messages": [m.dict() for m in body.messages]}]
        resp = await asyncio.to_thread(
            lambda: w.serving_endpoints.query(
                name=config.FRAUD_EXPLAIN_ENDPOINT,
                dataframe_records=payload,
            )
        )
    except Exception:
        log.exception("Fraud-explain agent call failed")
        raise HTTPException(status_code=502, detail="agent unavailable")

    pred = resp.predictions[0] if resp.predictions else ""
    if isinstance(pred, dict):
        ch = pred.get("choices") or []
        if ch and isinstance(ch[0], dict):
            text = (ch[0].get("message") or {}).get("content", str(pred))
        else:
            text = pred.get("content") or str(pred)
    else:
        text = str(pred)
    return {"role": "assistant", "content": text}


# --- Serve React frontend ---

if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{path:path}")
    async def serve_spa(path: str):
        index = STATIC_DIR / "index.html"
        if index.exists():
            return HTMLResponse(index.read_text())
        return HTMLResponse("<h1>Frontend not built. Run: cd frontend && npm run build</h1>")
else:
    @app.get("/")
    async def no_frontend():
        return HTMLResponse(
            "<h1>Frontend not built</h1>"
            "<p>Run: <code>cd app/frontend && npm install && npm run build</code></p>"
        )
