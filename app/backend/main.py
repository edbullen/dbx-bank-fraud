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

from fastapi import FastAPI, Query, Request
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
    """Background task: moves scored transactions from generator queue into Lakebase."""
    db = get_db()
    q = generator.subscribe()
    try:
        while True:
            txn = await q.get()
            db.insert_transaction(txn)
    except asyncio.CancelledError:
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
    get_db()

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


@app.post("/api/reset")
async def reset_data():
    from .generator import reset_id_counter
    generator.stop()
    generator.total_generated = 0
    reset_id_counter()
    get_db().clear_data()
    return {"status": "cleared"}


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
