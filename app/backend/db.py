"""
Database layer supporting two backends:
  - LakebaseDB: real Postgres via psycopg2 with Databricks OAuth token refresh
  - MockDB: in-memory store for offline development/testing

The active backend is chosen at startup based on config.is_lakebase_configured().
"""

import logging
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional, Protocol

import psycopg2
import psycopg2.extras
import psycopg2.pool

from . import config

log = logging.getLogger(__name__)

SCORED_TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS scored_transactions (
    id              TEXT PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    type            TEXT NOT NULL,
    amount          DOUBLE PRECISION NOT NULL,
    customer_id     INTEGER,
    firstname       TEXT,
    lastname        TEXT,
    country_orig    TEXT,
    country_dest    TEXT,
    fraud_prediction BOOLEAN NOT NULL,
    fraud_probability DOUBLE PRECISION,
    scored_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_scored_at ON scored_transactions (scored_at DESC);
CREATE INDEX IF NOT EXISTS idx_country_flow ON scored_transactions (country_orig, country_dest);
"""


# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------

class DB(Protocol):
    def insert_transaction(self, txn: dict) -> None: ...
    def get_latest_transactions(self, limit: int = 50) -> list[dict]: ...
    def get_transactions_since(self, since_iso: str) -> list[dict]: ...
    def get_metrics(self) -> dict: ...
    def get_country_flows(self) -> list[dict]: ...
    def get_time_series(self, bucket_seconds: int = 10) -> list[dict]: ...
    def ensure_tables(self) -> None: ...
    def clear_data(self) -> None: ...


# ---------------------------------------------------------------------------
# Lakebase (Postgres) backend
# ---------------------------------------------------------------------------

class LakebaseDB:
    """Connects to Lakebase Postgres with automatic OAuth token refresh."""

    def __init__(self):
        self._dsn = self._build_dsn()
        self._token_lock = threading.Lock()
        self._pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._refresh_thread: Optional[threading.Thread] = None
        self._running = True
        self._init_pool()

    def _build_dsn(self) -> str:
        password = self._get_token()
        return (
            f"host={config.LAKEBASE_HOST} "
            f"port={config.LAKEBASE_PORT} "
            f"dbname={config.LAKEBASE_DATABASE} "
            f"user={config.LAKEBASE_USER} "
            f"password={password} "
            f"sslmode={config.LAKEBASE_SSLMODE}"
        )

    def _get_token(self) -> str:
        """Get an OAuth token. Uses Databricks SDK for local dev, or PGPASSWORD for App mode."""
        pg_password = os.getenv("PGPASSWORD", "")
        if pg_password:
            return pg_password

        if not config.LAKEBASE_ENDPOINT:
            raise RuntimeError(
                "LAKEBASE_ENDPOINT must be set for OAuth token generation. "
                "Example: projects/retailfrauddb/branches/production/endpoints/primary"
            )

        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            credential = w.postgres.generate_database_credential(
                endpoint=config.LAKEBASE_ENDPOINT
            )
            log.info("OAuth token generated, expires at %s", credential.expire_time)
            return credential.token
        except Exception as e:
            raise RuntimeError(f"Failed to generate Lakebase OAuth token: {e}") from e

    def _init_pool(self):
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=5, dsn=self._dsn
        )
        self._refresh_thread = threading.Thread(
            target=self._token_refresh_loop, daemon=True
        )
        self._refresh_thread.start()

    def _token_refresh_loop(self):
        """Refresh the OAuth token and recreate the pool every 50 minutes."""
        while self._running:
            time.sleep(50 * 60)
            if not self._running:
                break
            try:
                log.info("Refreshing Lakebase OAuth token...")
                new_dsn = self._build_dsn()
                with self._token_lock:
                    old_pool = self._pool
                    self._pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=1, maxconn=5, dsn=new_dsn
                    )
                    self._dsn = new_dsn
                    if old_pool:
                        old_pool.closeall()
                log.info("Token refreshed, pool recreated")
            except Exception:
                log.exception("Failed to refresh OAuth token")

    def _conn(self):
        with self._token_lock:
            return self._pool.getconn()

    def _put(self, conn):
        with self._token_lock:
            self._pool.putconn(conn)

    def ensure_tables(self):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(SCORED_TRANSACTIONS_DDL)
            conn.commit()
            log.info("Lakebase tables ensured")
        finally:
            self._put(conn)

    def insert_transaction(self, txn: dict) -> None:
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO scored_transactions
                       (id, timestamp, type, amount, customer_id,
                        firstname, lastname, country_orig, country_dest,
                        fraud_prediction, fraud_probability, scored_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (id) DO NOTHING""",
                    (
                        txn["id"], txn["timestamp"], txn["type"], txn["amount"],
                        txn.get("customer_id"), txn.get("firstname"), txn.get("lastname"),
                        txn["country_orig"], txn["country_dest"],
                        txn["fraud_prediction"], txn["fraud_probability"],
                        txn["scored_at"],
                    ),
                )
            conn.commit()
        finally:
            self._put(conn)

    def get_latest_transactions(self, limit: int = 50) -> list[dict]:
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM scored_transactions ORDER BY scored_at DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
                return [_row_to_dict(r) for r in rows]
        finally:
            self._put(conn)

    def get_transactions_since(self, since_iso: str) -> list[dict]:
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM scored_transactions WHERE scored_at > %s ORDER BY scored_at",
                    (since_iso,),
                )
                return [_row_to_dict(r) for r in cur.fetchall()]
        finally:
            self._put(conn)

    def get_metrics(self) -> dict:
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) AS total_transactions,
                        COALESCE(SUM(CASE WHEN fraud_prediction THEN 1 ELSE 0 END), 0) AS fraud_count,
                        COALESCE(SUM(CASE WHEN NOT fraud_prediction THEN 1 ELSE 0 END), 0) AS legit_count,
                        CASE WHEN COUNT(*) > 0
                            THEN ROUND(SUM(CASE WHEN fraud_prediction THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2)
                            ELSE 0 END AS fraud_rate,
                        COALESCE(ROUND(SUM(amount)::numeric, 2), 0) AS total_amount
                    FROM scored_transactions
                """)
                row = cur.fetchone()
                return dict(row) if row else {
                    "total_transactions": 0, "fraud_count": 0,
                    "legit_count": 0, "fraud_rate": 0.0, "total_amount": 0.0,
                }
        finally:
            self._put(conn)

    def get_country_flows(self) -> list[dict]:
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        country_orig AS source,
                        country_dest AS target,
                        COUNT(*) AS total_count,
                        SUM(CASE WHEN fraud_prediction THEN 1 ELSE 0 END) AS fraud_count,
                        ROUND(SUM(amount)::numeric, 2) AS total_amount
                    FROM scored_transactions
                    GROUP BY country_orig, country_dest
                    ORDER BY total_count DESC
                    LIMIT 30
                """)
                return [dict(r) for r in cur.fetchall()]
        finally:
            self._put(conn)

    def get_time_series(self, bucket_seconds: int = 10) -> list[dict]:
        conn = self._conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        (EXTRACT(EPOCH FROM timestamp)::bigint / %s * %s) AS timestamp,
                        COUNT(*) AS total,
                        SUM(CASE WHEN fraud_prediction THEN 1 ELSE 0 END) AS fraud,
                        SUM(CASE WHEN NOT fraud_prediction THEN 1 ELSE 0 END) AS legit
                    FROM scored_transactions
                    GROUP BY 1
                    ORDER BY 1 DESC
                    LIMIT 60
                """, (bucket_seconds, bucket_seconds))
                rows = [dict(r) for r in cur.fetchall()]
                rows.reverse()
                return rows
        finally:
            self._put(conn)

    def clear_data(self):
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE scored_transactions")
            conn.commit()
            log.info("Lakebase scored_transactions truncated")
        finally:
            self._put(conn)

    def shutdown(self):
        self._running = False
        if self._pool:
            self._pool.closeall()


def _row_to_dict(row: dict) -> dict:
    """Convert psycopg2 RealDictRow to a JSON-serializable dict."""
    result = dict(row)
    for k, v in result.items():
        if isinstance(v, datetime):
            result[k] = v.isoformat()
    return result


# ---------------------------------------------------------------------------
# MockDB (in-memory, for offline testing)
# ---------------------------------------------------------------------------

class MockDB:
    """In-memory store that mimics the Lakebase query interface."""

    def __init__(self, max_transactions: int = 5000):
        self._transactions: deque = deque(maxlen=max_transactions)
        self._lock = threading.Lock()

    def ensure_tables(self):
        pass

    def insert_transaction(self, txn: dict):
        with self._lock:
            self._transactions.append(txn)

    def get_latest_transactions(self, limit: int = 50) -> list[dict]:
        with self._lock:
            items = list(self._transactions)
            return list(reversed(items[-limit:]))

    def get_transactions_since(self, since_iso: str) -> list[dict]:
        with self._lock:
            return [t for t in self._transactions if t["scored_at"] > since_iso]

    def get_metrics(self) -> dict:
        with self._lock:
            items = list(self._transactions)
        total = len(items)
        if total == 0:
            return {"total_transactions": 0, "fraud_count": 0, "fraud_rate": 0.0,
                    "total_amount": 0.0, "legit_count": 0}
        fraud_count = sum(1 for t in items if t["fraud_prediction"])
        total_amount = sum(t["amount"] for t in items)
        return {
            "total_transactions": total,
            "fraud_count": fraud_count,
            "legit_count": total - fraud_count,
            "fraud_rate": round(fraud_count / total * 100, 2),
            "total_amount": round(total_amount, 2),
        }

    def get_country_flows(self) -> list[dict]:
        with self._lock:
            items = list(self._transactions)
        flows: dict[tuple[str, str], dict] = {}
        for t in items:
            key = (t["country_orig"], t["country_dest"])
            if key not in flows:
                flows[key] = {"source": key[0], "target": key[1],
                              "total_count": 0, "fraud_count": 0, "total_amount": 0.0}
            flows[key]["total_count"] += 1
            flows[key]["total_amount"] += t["amount"]
            if t["fraud_prediction"]:
                flows[key]["fraud_count"] += 1
        result = sorted(flows.values(), key=lambda f: f["total_count"], reverse=True)
        return result[:30]

    def get_time_series(self, bucket_seconds: int = 10) -> list[dict]:
        with self._lock:
            items = list(self._transactions)
        if not items:
            return []
        buckets: dict[int, dict] = {}
        for t in items:
            ts = datetime.fromisoformat(t["timestamp"])
            bucket_key = int(ts.timestamp()) // bucket_seconds * bucket_seconds
            if bucket_key not in buckets:
                buckets[bucket_key] = {"timestamp": bucket_key, "total": 0, "fraud": 0, "legit": 0}
            buckets[bucket_key]["total"] += 1
            if t["fraud_prediction"]:
                buckets[bucket_key]["fraud"] += 1
            else:
                buckets[bucket_key]["legit"] += 1
        return sorted(buckets.values(), key=lambda b: b["timestamp"])[-60:]

    def clear_data(self):
        with self._lock:
            self._transactions.clear()

    def shutdown(self):
        pass


# ---------------------------------------------------------------------------
# Singleton accessor
# ---------------------------------------------------------------------------

_db: Optional[DB] = None


def get_db() -> DB:
    global _db
    if _db is None:
        if config.is_lakebase_configured():
            log.info("Connecting to Lakebase at %s", config.LAKEBASE_HOST)
            _db = LakebaseDB()
            _db.ensure_tables()
            log.info("Lakebase connected, tables ensured")
        else:
            log.info("Using in-memory MockDB (set LAKEBASE_HOST to use Lakebase)")
            _db = MockDB()
    return _db


def shutdown_db():
    global _db
    if _db is not None:
        _db.shutdown()
        _db = None
