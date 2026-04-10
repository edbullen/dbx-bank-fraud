#!/usr/bin/env python3
"""
One-time profiling script for the fraud demo data generator.

Queries gold_transactions via the Databricks SQL Statement API to extract
feature distribution profiles, then saves:
  - app/data/distributions.json  (numeric + categorical distributions)
  - app/data/customers.json      (compact customer lookup from local NDJSON)

Usage:
    # Set vars in app/.env first (UNITY_CATALOG, UNITY_SCHEMA, SQL_WAREHOUSE_ID)
    python scripts/profile_training_data.py

    # Or pass via CLI:
    python scripts/profile_training_data.py \
        --catalog hsbc_fraud_ai_ml_catalog \
        --schema retail-bank \
        --warehouse <warehouse-id>
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
APP_DATA_DIR = REPO_ROOT / "app" / "data"
CUSTOMERS_SRC = REPO_ROOT / "data" / "customers_json" / "banking_customers.json"

NUMERIC_FEATURES = ["amount", "newBalanceDest", "oldBalanceDest", "diffOrig", "diffDest"]


def run_sql(client, warehouse_id: str, sql: str) -> list[dict]:
    """Execute SQL via Statement API and return rows as list of dicts."""
    from databricks.sdk.service.sql import StatementState, Disposition

    log.info("Executing SQL (first 120 chars): %s...", sql.replace("\n", " ")[:120])
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
    )

    if response.status.state == StatementState.FAILED:
        raise RuntimeError(f"SQL failed: {response.status.error}")

    # Poll if the query hasn't finished within the initial wait window
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        log.info("  waiting for query to complete...")
        time.sleep(5)
        response = client.statement_execution.get_statement(response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL ended in state {response.status.state}: {response.status.error}")

    columns = [c.name for c in response.manifest.schema.columns]
    rows = response.result.data_array or []
    return [dict(zip(columns, row)) for row in rows]


def profile_numeric_features(client, warehouse_id: str, table_fqn: str) -> dict:
    """Profile numeric features grouped by (type, is_fraud)."""
    percentile_exprs = []
    for feat in NUMERIC_FEATURES:
        percentile_exprs.extend([
            f"CAST(AVG(CAST({feat} AS DOUBLE)) AS DOUBLE) AS {feat}_mean",
            f"CAST(STDDEV(CAST({feat} AS DOUBLE)) AS DOUBLE) AS {feat}_stddev",
            f"CAST(MIN(CAST({feat} AS DOUBLE)) AS DOUBLE) AS {feat}_min",
            f"CAST(MAX(CAST({feat} AS DOUBLE)) AS DOUBLE) AS {feat}_max",
            f"CAST(PERCENTILE_APPROX(CAST({feat} AS DOUBLE), 0.10) AS DOUBLE) AS {feat}_p10",
            f"CAST(PERCENTILE_APPROX(CAST({feat} AS DOUBLE), 0.25) AS DOUBLE) AS {feat}_p25",
            f"CAST(PERCENTILE_APPROX(CAST({feat} AS DOUBLE), 0.50) AS DOUBLE) AS {feat}_p50",
            f"CAST(PERCENTILE_APPROX(CAST({feat} AS DOUBLE), 0.75) AS DOUBLE) AS {feat}_p75",
            f"CAST(PERCENTILE_APPROX(CAST({feat} AS DOUBLE), 0.90) AS DOUBLE) AS {feat}_p90",
        ])

    sql = f"""
        SELECT
            type,
            CAST(is_fraud AS INT) AS is_fraud,
            COUNT(*) AS count,
            {', '.join(percentile_exprs)}
        FROM {table_fqn}
        GROUP BY type, CAST(is_fraud AS INT)
        ORDER BY type, is_fraud
    """

    rows = run_sql(client, warehouse_id, sql)
    profiles = {}
    for row in rows:
        key = f"{row['type']}_{row['is_fraud']}"
        entry = {"type": row["type"], "is_fraud": int(row["is_fraud"]), "count": int(row["count"])}
        for feat in NUMERIC_FEATURES:
            entry[feat] = {
                "mean": _float(row.get(f"{feat}_mean")),
                "stddev": _float(row.get(f"{feat}_stddev")),
                "min": _float(row.get(f"{feat}_min")),
                "max": _float(row.get(f"{feat}_max")),
                "p10": _float(row.get(f"{feat}_p10")),
                "p25": _float(row.get(f"{feat}_p25")),
                "p50": _float(row.get(f"{feat}_p50")),
                "p75": _float(row.get(f"{feat}_p75")),
                "p90": _float(row.get(f"{feat}_p90")),
            }
        profiles[key] = entry

    return profiles


def profile_type_frequencies(client, warehouse_id: str, table_fqn: str) -> dict:
    """Get transaction type frequency distribution."""
    sql = f"""
        SELECT type, COUNT(*) AS count,
               SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count
        FROM {table_fqn}
        GROUP BY type
        ORDER BY count DESC
    """
    rows = run_sql(client, warehouse_id, sql)
    total = sum(int(r["count"]) for r in rows)
    return {
        r["type"]: {
            "count": int(r["count"]),
            "fraud_count": int(r["fraud_count"]),
            "weight": round(int(r["count"]) / total, 6) if total > 0 else 0,
        }
        for r in rows
    }


def profile_country_pairs(client, warehouse_id: str, table_fqn: str) -> list[dict]:
    """Get top country pair frequencies with fraud rate."""
    sql = f"""
        SELECT
            countryOrig_name,
            countryDest_name,
            COUNT(*) AS count,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS fraud_count
        FROM {table_fqn}
        GROUP BY countryOrig_name, countryDest_name
        ORDER BY count DESC
        LIMIT 80
    """
    rows = run_sql(client, warehouse_id, sql)
    total = sum(int(r["count"]) for r in rows)
    return [
        {
            "countryOrig_name": r["countryOrig_name"],
            "countryDest_name": r["countryDest_name"],
            "count": int(r["count"]),
            "fraud_count": int(r["fraud_count"]),
            "weight": round(int(r["count"]) / total, 6) if total > 0 else 0,
            "fraud_rate": round(int(r["fraud_count"]) / int(r["count"]), 4) if int(r["count"]) > 0 else 0,
        }
        for r in rows
    ]


def profile_overall_stats(client, warehouse_id: str, table_fqn: str) -> dict:
    """Get overall dataset stats."""
    sql = f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS total_fraud
        FROM {table_fqn}
    """
    rows = run_sql(client, warehouse_id, sql)
    row = rows[0]
    total = int(row["total"])
    fraud = int(row["total_fraud"])
    return {
        "total_transactions": total,
        "total_fraud": fraud,
        "fraud_rate": round(fraud / total, 6) if total > 0 else 0,
    }


def extract_customers(src: Path, dst: Path):
    """Extract compact customer lookup (id, firstname, lastname, country) from NDJSON."""
    log.info("Extracting customers from %s", src)
    customers = []
    with open(src) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            customers.append({
                "id": record["id"],
                "firstname": record["firstname"],
                "lastname": record["lastname"],
                "country": record.get("country", ""),
            })

    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(dst, "w") as f:
        json.dump(customers, f, separators=(",", ":"))
    log.info("Wrote %d customers to %s (%.1f KB)", len(customers), dst, dst.stat().st_size / 1024)


def _float(val) -> float:
    """Safely cast to float, defaulting to 0.0."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def main():
    parser = argparse.ArgumentParser(description="Profile gold_transactions for the fraud demo generator")
    parser.add_argument("--catalog", default=os.getenv("UNITY_CATALOG", "hsbc_fraud_ai_ml_catalog"))
    parser.add_argument("--schema", default=os.getenv("UNITY_SCHEMA", "retail-bank"))
    parser.add_argument("--warehouse", default=os.getenv("SQL_WAREHOUSE_ID", ""))
    parser.add_argument("--customers-only", action="store_true", help="Only extract customers, skip SQL profiling")
    args = parser.parse_args()

    # Always extract customers (no SQL Warehouse needed)
    customers_dst = APP_DATA_DIR / "customers.json"
    extract_customers(CUSTOMERS_SRC, customers_dst)

    if args.customers_only:
        log.info("--customers-only: skipping SQL profiling")
        return

    if not args.warehouse:
        log.error("SQL_WAREHOUSE_ID not set. Pass --warehouse <id> or set SQL_WAREHOUSE_ID in .env")
        sys.exit(1)

    table_fqn = f"`{args.catalog}`.`{args.schema}`.gold_transactions"
    log.info("Profiling %s via warehouse %s", table_fqn, args.warehouse)

    from databricks.sdk import WorkspaceClient
    client = WorkspaceClient()

    profile = {
        "metadata": {
            "catalog": args.catalog,
            "schema": args.schema,
            "table": "gold_transactions",
            "profiled_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "overall": profile_overall_stats(client, args.warehouse, table_fqn),
        "type_frequencies": profile_type_frequencies(client, args.warehouse, table_fqn),
        "numeric_profiles": profile_numeric_features(client, args.warehouse, table_fqn),
        "country_pairs": profile_country_pairs(client, args.warehouse, table_fqn),
    }

    dst = APP_DATA_DIR / "distributions.json"
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(dst, "w") as f:
        json.dump(profile, f, indent=2)
    log.info("Wrote distribution profile to %s (%.1f KB)", dst, dst.stat().st_size / 1024)
    log.info("Overall stats: %s", json.dumps(profile["overall"]))


if __name__ == "__main__":
    # Load .env if present (for DATABRICKS_CONFIG_PROFILE, UNITY_CATALOG, etc.)
    env_path = REPO_ROOT / "app" / ".env"
    if env_path.exists():
        from dotenv import load_dotenv
        load_dotenv(env_path)
        log.info("Loaded .env from %s", env_path)

    main()
