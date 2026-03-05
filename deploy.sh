#!/usr/bin/env bash
# Deploy and build: fraud_model* notebooks (import + run training then deploy) and Retail Bank Fraud dashboard.
# Use undeploy.sh to remove deployed artefacts.
# Usage: ./deploy.sh [OPTIONS]; see --help.

set -e

ROOT="$(cd "$(dirname "$0")" && git rev-parse --show-toplevel 2>/dev/null)" || ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

PROFILE=""
HOST=""
CATALOG=""
SCHEMA=""
WORKSPACE_PATH=""
WAREHOUSE_ID=""
CLUSTER_ID=""
NON_INTERACTIVE=false
SKIP_ML=false
SKIP_DASHBOARD=false
SKIP_NOTEBOOKS=false
DASHBOARD_JSON="$ROOT/dashboards/Retail_Bank_Fraud_Dashboard.lvdash.json"

usage() {
  cat <<'EOF'
Usage: deploy.sh [OPTIONS]

  -p, --profile PROFILE       Databricks CLI profile name
  --host HOST                 Workspace URL
  -c, --catalog CATALOG       Unity Catalog (for ML widgets and dashboard datasets)
  -s, --schema SCHEMA         Unity Schema (for ML widgets and dashboard datasets)
  --workspace-path PATH       Workspace path for notebooks (e.g. /Users/you@example.com/dbx-bank-fraud)
  --warehouse-id ID           SQL warehouse ID for the Lakeview dashboard (required for dashboard)
  --cluster-id ID             Optional: existing cluster ID for ML notebooks; if omitted, uses serverless compute
  --skip-ml                   Do not run training or deploy notebooks (still imports if not --skip-notebooks)
  --skip-notebooks            Do not import fraud_model* notebooks to workspace (dashboard-only deploy)
  --skip-dashboard            Do not create or publish the dashboard
  -y, --yes, --non-interactive  No prompts; fail if required params missing
  -i, --interactive           Prompt for missing params
  -h, --help                  Show this help and exit

Deploys: fraud_model_training.py, fraud_model_deploy.py (and fraud_model_run.py) to workspace,
runs training then deploy to build the model in UC; creates and publishes the Retail Bank Fraud
dashboard from dashboards/Retail_Bank_Fraud_Dashboard.lvdash.json.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--profile)       PROFILE="$2"; shift 2 ;;
    --host)             HOST="$2"; shift 2 ;;
    -c|--catalog)       CATALOG="$2"; shift 2 ;;
    -s|--schema)        SCHEMA="$2"; shift 2 ;;
    --workspace-path)   WORKSPACE_PATH="$2"; shift 2 ;;
    --warehouse-id)     WAREHOUSE_ID="$2"; shift 2 ;;
    --cluster-id)       CLUSTER_ID="$2"; shift 2 ;;
    --skip-ml)          SKIP_ML=true; shift ;;
    --skip-notebooks)   SKIP_NOTEBOOKS=true; shift ;;
    --skip-dashboard)   SKIP_DASHBOARD=true; shift ;;
    -y|--yes|--non-interactive) NON_INTERACTIVE=true; shift ;;
    -i|--interactive)   NON_INTERACTIVE=false; shift ;;
    -h|--help)          usage; exit 0 ;;
    *)                  echo "Unknown option: $1"; usage; exit 1 ;;
  esac
done

dbx() {
  if [[ -n "$PROFILE" ]]; then
    databricks -p "$PROFILE" "$@"
  else
    databricks "$@"
  fi
}

require_param() {
  local name="$1"
  local val="$2"
  if [[ -z "$val" ]]; then
    echo "Error: $name is required."
    if $NON_INTERACTIVE; then
      usage
      exit 1
    fi
    return 1
  fi
  return 0
}

prompt_val() {
  local name="$1"
  local default="$2"
  local var_ref="$3"
  local prompt="$name"
  [[ -n "$default" ]] && prompt="$prompt [$default]"
  read -r -p "$prompt: " input
  [[ -z "$input" && -n "$default" ]] && input="$default"
  printf -v "$var_ref" '%s' "$input"
}

# --- Auth ---
echo "=== Databricks CLI profiles ==="
if ! databricks auth profiles 2>/dev/null; then
  echo "Could not list profiles. Is the Databricks CLI installed?"
  exit 1
fi

if $NON_INTERACTIVE; then
  if [[ -z "$PROFILE" && -z "$HOST" ]]; then
    echo "Error: In non-interactive mode either --profile or --host is required."
    usage
    exit 1
  fi
  [[ -z "$PROFILE" && -n "$HOST" ]] && { databricks auth login --host "$HOST" || true; PROFILE="DEFAULT"; }
  [[ -z "$PROFILE" ]] && PROFILE="DEFAULT"
else
  if [[ -z "$PROFILE" ]]; then
    read -r -p "Enter profile name or press Enter to login with host: " prof_in
    if [[ -n "$prof_in" ]]; then
      PROFILE="$prof_in"
    else
      read -r -p "Workspace URL: " HOST
      [[ -z "$HOST" ]] && { echo "Host required."; exit 1; }
      databricks auth login --host "$HOST" || true
      PROFILE="DEFAULT"
    fi
  fi
fi

echo "Verifying auth for profile: $PROFILE"
if ! dbx workspace list / &>/dev/null; then
  if $NON_INTERACTIVE; then
    echo "Error: Auth verification failed. Run: databricks auth login -p $PROFILE"
    exit 1
  fi
  echo "Auth check failed. Run: databricks auth login -p $PROFILE"
  read -r -p "Retry after login? [y/N]: " retry
  [[ "${retry,,}" != "y" && "${retry,,}" != "yes" ]] && exit 1
  if ! dbx workspace list / &>/dev/null; then
    echo "Verification still failed."
    exit 1
  fi
fi
export DATABRICKS_CONFIG_PROFILE="$PROFILE"

while ! require_param "catalog" "$CATALOG"; do prompt_val "Unity Catalog name" "" CATALOG; done
while ! require_param "schema" "$SCHEMA"; do prompt_val "Unity Schema name" "" SCHEMA; done
# workspace-path required only when deploying notebooks or running ML (need notebook paths)
if ! $SKIP_NOTEBOOKS || ! $SKIP_ML; then
  while ! require_param "workspace-path" "$WORKSPACE_PATH"; do prompt_val "Workspace path for notebooks (e.g. /Users/you@example.com/dbx-bank-fraud)" "" WORKSPACE_PATH; done
fi

if ! $SKIP_ML; then
  # cluster-id is optional; when empty we use serverless compute (no prompt required)
  if [[ -z "$CLUSTER_ID" ]] && ! $NON_INTERACTIVE; then
    read -r -p "Existing cluster ID (optional; press Enter for serverless compute): " CLUSTER_ID
  fi
fi

if ! $SKIP_DASHBOARD; then
  while ! require_param "warehouse-id" "$WAREHOUSE_ID"; do prompt_val "SQL warehouse ID (for dashboard)" "" WAREHOUSE_ID; done
fi

# --- Deploy notebooks (fraud_model*) ---
if ! $SKIP_NOTEBOOKS; then
  echo "=== Deploying fraud_model* notebooks to workspace ==="
  NOTES_DIR="$ROOT/notebooks"
  for f in fraud_model_training fraud_model_deploy fraud_model_run; do
    src="$NOTES_DIR/${f}.py"
    if [[ -f "$src" ]]; then
      dbx workspace import "$WORKSPACE_PATH/notebooks/$f" --file "$src" --language PYTHON --format SOURCE --overwrite
      echo "Imported $f"
    fi
  done
else
  echo "=== Skipping notebook import (--skip-notebooks) ==="
fi

# --- Build ML: run training then deploy ---
if ! $SKIP_ML; then
  TRAIN_PATH="$WORKSPACE_PATH/notebooks/fraud_model_training"
  DEPLOY_PATH="$WORKSPACE_PATH/notebooks/fraud_model_deploy"

  submit_notebook() {
    local path="$1"
    local run_name="$2"
    local json_file
    json_file="$(mktemp)"
    if [[ -n "$CLUSTER_ID" ]]; then
      # Use existing all-purpose cluster
      cat <<SUB >"$json_file"
{
  "run_name": "$run_name",
  "tasks": [
    {
      "task_key": "main",
      "notebook_task": {
        "notebook_path": "$path",
        "base_parameters": {
          "unity_catalog": "$CATALOG",
          "unity_schema": "$SCHEMA"
        }
      },
      "existing_cluster_id": "$CLUSTER_ID"
    }
  ]
}
SUB
    else
      # Serverless compute: omit cluster so the platform uses serverless by default
      # (supported for notebook tasks when Unity Catalog is enabled).
      # If your workspace requires explicit compute, use --cluster-id with an existing cluster.
      cat <<SUB >"$json_file"
{
  "run_name": "$run_name",
  "tasks": [
    {
      "task_key": "main",
      "notebook_task": {
        "notebook_path": "$path",
        "base_parameters": {
          "unity_catalog": "$CATALOG",
          "unity_schema": "$SCHEMA"
        }
      }
    }
  ]
}
SUB
    fi
    dbx jobs submit --json @"$json_file" --timeout 30m
    rm -f "$json_file"
  }

  if [[ -n "$CLUSTER_ID" ]]; then
    echo "=== Using existing cluster: $CLUSTER_ID ==="
  else
    echo "=== Using serverless compute (no cluster specified) ==="
  fi
  echo "=== Running fraud_model_training notebook ==="
  submit_notebook "$TRAIN_PATH" "fraud-model-training"

  echo "=== Running fraud_model_deploy notebook ==="
  submit_notebook "$DEPLOY_PATH" "fraud-model-deploy"
fi

# --- Dashboard: create from JSON and publish ---
if ! $SKIP_DASHBOARD && [[ -f "$DASHBOARD_JSON" ]]; then
  echo "=== Creating and publishing dashboard ==="
  DASH_TMP="$(mktemp)"
  REQ_TMP="$(mktemp)"
  sed -e "s/\"catalog\": \"main\"/\"catalog\": \"$CATALOG\"/g" \
      -e "s/\"schema\": \"ed_bullen\"/\"schema\": \"$SCHEMA\"/g" \
      "$DASHBOARD_JSON" > "$DASH_TMP"
  python3 <<PY
import json, sys
dash_file, req_file = "$DASH_TMP", "$REQ_TMP"
with open(dash_file) as f:
    ser = f.read()
req = {
  "display_name": "Retail Bank Fraud Dashboard",
  "warehouse_id": "$WAREHOUSE_ID",
  "dataset_catalog": "$CATALOG",
  "dataset_schema": "$SCHEMA",
  "serialized_dashboard": ser
}
with open(req_file, "w") as f:
    json.dump(req, f)
PY
  CREATE_OUT="$(dbx lakeview create --json @"$REQ_TMP" -o json 2>/dev/null)" || true
  rm -f "$DASH_TMP" "$REQ_TMP"

  if [[ -n "$CREATE_OUT" ]]; then
    DASHBOARD_ID="$(echo "$CREATE_OUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('dashboard_id') or d.get('id') or '')" 2>/dev/null)"
    if [[ -n "$DASHBOARD_ID" ]]; then
      echo "Publishing dashboard $DASHBOARD_ID"
      dbx lakeview publish "$DASHBOARD_ID" --warehouse-id "$WAREHOUSE_ID" 2>/dev/null || true
      echo "{\"dashboard_id\": \"$DASHBOARD_ID\", \"workspace_path\": \"$WORKSPACE_PATH\"}" > "$ROOT/.deploy-state.json"
    fi
  else
    echo "Note: Dashboard create failed. Create manually from dashboards/Retail_Bank_Fraud_Dashboard.lvdash.json (Import dashboard in the UI) if needed."
  fi
elif ! $SKIP_DASHBOARD && [[ ! -f "$DASHBOARD_JSON" ]]; then
  echo "Warning: Dashboard template not found at $DASHBOARD_JSON; skipping dashboard."
fi

echo "=== deploy.sh finished ==="
