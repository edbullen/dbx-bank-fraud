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
GENIE=false
SERVE_MODEL=false
ENDPOINT_NAME="bank-fraud-predict"
MODEL_NAME="bank_fraud_predict"
MODEL_VERSION=""   # empty = use production alias or latest version
DASHBOARD_JSON="$ROOT/dashboards/Retail_Bank_Fraud_Dashboard.lvdash.json"

usage() {
  cat <<'EOF'
Usage: deploy.sh [OPTIONS]

  -p, --profile PROFILE       Databricks CLI profile name
  --host HOST                 Workspace URL
  -c, --catalog CATALOG       Unity Catalog (for ML widgets and dashboard datasets)
  -s, --schema SCHEMA         Unity Schema (for ML widgets and dashboard datasets)
  --workspace-path PATH       Workspace path for notebooks (required only when importing or running ML; use --skip-notebooks --skip-ml to omit)
  --warehouse-id ID           SQL warehouse ID for the Lakeview dashboard (required only when creating dashboard or Genie)
  --cluster-id ID             Optional: existing cluster ID for ML notebooks; if omitted, uses serverless compute
  --skip-ml                   Do not run training or deploy notebooks (still imports if not --skip-notebooks)
  --skip-notebooks            Do not import fraud_model* notebooks to workspace (dashboard-only or serve-model-only deploy)
  --skip-dashboard            Do not create or publish the dashboard
  --genie                     Create a Genie space for gold_transactions (uses same warehouse as dashboard)
  --serve-model               Create a model serving endpoint (requires model in UC)
  --endpoint-name NAME        Name of the serving endpoint in the workspace (default: bank-fraud-predict)
  --model-name NAME           Unity Catalog model name to serve (default: bank_fraud_predict). Full name is catalog.schema.model_name
  --model-version VER         Model version to serve (default: version with alias production, else latest)
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
    --genie)            GENIE=true; shift ;;
    --serve-model)      SERVE_MODEL=true; shift ;;
    --endpoint-name)    ENDPOINT_NAME="$2"; shift 2 ;;
    --model-name)       MODEL_NAME="$2"; shift 2 ;;
    --model-version)    MODEL_VERSION="$2"; shift 2 ;;
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

# warehouse-id when creating dashboard or Genie, but not in serve-model-only mode (--serve-model with --skip-notebooks --skip-ml)
if ( (! $SKIP_DASHBOARD) || $GENIE ) && ! ( $SERVE_MODEL && $SKIP_NOTEBOOKS && $SKIP_ML ); then
  if ! $SKIP_DASHBOARD; then
    while ! require_param "warehouse-id" "$WAREHOUSE_ID"; do prompt_val "SQL warehouse ID (for dashboard)" "" WAREHOUSE_ID; done
  fi
  if $GENIE && [[ -z "$WAREHOUSE_ID" ]]; then
    while ! require_param "warehouse-id" "$WAREHOUSE_ID"; do prompt_val "SQL warehouse ID (for Genie space)" "" WAREHOUSE_ID; done
  fi
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
    local submit_out
    submit_out="$(dbx jobs submit --json @"$json_file" -o json --no-wait 2>/dev/null)" || true
    rm -f "$json_file"
    local run_id
    run_id="$(echo "$submit_out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('run_id',''))" 2>/dev/null)"
    if [[ -z "$run_id" ]]; then
      echo "Error: Failed to submit notebook run (no run_id). Check CLI and workspace access."
      exit 1
    fi
    echo "Run ID: $run_id (if this run fails, view error output with: databricks -p $PROFILE jobs get-run-output $run_id)"
    local state="" result_state="" deadline
    deadline=$(($(date +%s) + 1800))   # 30 min
    while [[ $(date +%s) -lt $deadline ]]; do
      local run_json
      run_json="$(dbx jobs get-run "$run_id" -o json 2>/dev/null)" || true
      state="$(echo "$run_json" | python3 -c "
import sys,json
d=json.load(sys.stdin)
s=d.get('state') or d.get('run_state') or {}
print(s.get('life_cycle_state') or d.get('life_cycle_state') or '')
" 2>/dev/null)"
      result_state="$(echo "$run_json" | python3 -c "
import sys,json
d=json.load(sys.stdin)
s=d.get('state') or d.get('run_state') or {}
print(s.get('result_state') or s.get('state_message') or '')
" 2>/dev/null)"
      [[ -z "$state" ]] && state="UNKNOWN"
      if [[ "$state" == "TERMINATED" || "$state" == "SKIPPED" ]]; then
        if [[ "$result_state" == "FAILED" || "$result_state" == "INTERNAL_ERROR" ]]; then
          echo "Run failed (state: $state, result: $result_state). Fetching run output:"
          echo "---"
          dbx jobs get-run-output "$run_id" 2>/dev/null || true
          echo "---"
          echo "To view full run details: databricks -p $PROFILE jobs get-run $run_id"
          exit 1
        fi
        return 0
      fi
      if [[ "$state" == "INTERNAL_ERROR" || "$state" == "FAILED" ]]; then
        echo "Run failed (state: $state). Fetching run output:"
        echo "---"
        dbx jobs get-run-output "$run_id" 2>/dev/null || true
        echo "---"
        echo "To view full run details: databricks -p $PROFILE jobs get-run $run_id"
        exit 1
      fi
      sleep 15
    done
    echo "Error: Run did not complete within 30 minutes (last state: $state). View output: databricks -p $PROFILE jobs get-run-output $run_id"
    exit 1
  }

  if [[ -n "$CLUSTER_ID" ]]; then
    echo "=== Using existing cluster: $CLUSTER_ID ==="
  else
    echo "=== Using serverless compute (no cluster specified) ==="
  fi
  echo "=== Running fraud_model_training notebook (** expect 20 to 30 minutes to complete **) ==="
  submit_notebook "$TRAIN_PATH" "fraud-model-training"

  echo "=== Running fraud_model_deploy notebook ==="
  submit_notebook "$DEPLOY_PATH" "fraud-model-deploy"
fi

# --- Model serving endpoint: create endpoint for UC model ---
if $SERVE_MODEL; then
  echo "=== Creating model serving endpoint ==="
  MODEL_FULL_NAME="$CATALOG.$SCHEMA.$MODEL_NAME"
  VERSION_TO_SERVE="$MODEL_VERSION"
  if [[ -z "$VERSION_TO_SERVE" ]]; then
    ALIAS_OUT="$(dbx model-versions get-by-alias "$MODEL_FULL_NAME" production -o json 2>/dev/null)" || true
    if [[ -n "$ALIAS_OUT" ]]; then
      VERSION_TO_SERVE="$(echo "$ALIAS_OUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(str(d.get('version','')))" 2>/dev/null)"
    fi
    if [[ -z "$VERSION_TO_SERVE" ]]; then
      LIST_OUT="$(dbx model-versions list "$MODEL_FULL_NAME" --max-results 50 -o json 2>/dev/null)" || true
      VERSION_TO_SERVE="$(echo "$LIST_OUT" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    vers = d if isinstance(d, list) else d.get('model_versions') or d.get('versions') or []
    if not vers and isinstance(d, dict) and 'model_versions' in d: vers = d['model_versions']
    nums = []
    for v in vers:
        if not v: continue
        n = v.get('version') or v.get('name') or 0
        try: nums.append(int(n))
        except (TypeError, ValueError): pass
    print(str(max(nums)) if nums else '')
except Exception:
    print('')
" 2>/dev/null)"
    fi
  fi
  if [[ -z "$VERSION_TO_SERVE" ]]; then
    echo "Warning: Could not resolve model version for $MODEL_FULL_NAME (set --model-version or ensure model is registered with production alias). Skipping serving endpoint."
  else
    SERVING_JSON="$(mktemp)"
    python3 <<PY
import json
payload = {
  "name": "$ENDPOINT_NAME",
  "config": {
    "served_entities": [
      {
        "name": "current",
        "entity_name": "$MODEL_FULL_NAME",
        "entity_version": "$VERSION_TO_SERVE",
        "workload_size": "Small",
        "scale_to_zero_enabled": True
      }
    ]
  }
}
with open("$SERVING_JSON", "w") as f:
    json.dump(payload, f)
PY
    CREATE_ERR=""
    if ! CREATE_OUT="$(dbx serving-endpoints create --json @"$SERVING_JSON" --no-wait 2>&1)"; then
      CREATE_ERR="$CREATE_OUT"
    fi
    if [[ -z "$CREATE_ERR" ]]; then
      echo "Serving endpoint '$ENDPOINT_NAME' created (building in background)."
      STATE_FILE="$ROOT/.deploy-state.json"
      if [[ -f "$STATE_FILE" ]]; then
        python3 <<PY
import json
with open("$STATE_FILE") as f:
    d = json.load(f)
d["serving_endpoint_name"] = "$ENDPOINT_NAME"
with open("$STATE_FILE", "w") as f:
    json.dump(d, f)
PY
      else
        echo "{\"serving_endpoint_name\": \"$ENDPOINT_NAME\", \"workspace_path\": \"$WORKSPACE_PATH\"}" > "$STATE_FILE"
      fi
    else
      echo "Serving endpoint create failed: $CREATE_ERR"
      if echo "$CREATE_ERR" | grep -qi "already exists"; then
        echo "Delete the existing endpoint first or use a different --endpoint-name."
      fi
    fi
    rm -f "$SERVING_JSON"
  fi
fi

# --- Dashboard: create from JSON and publish (only when we have a warehouse-id) ---
if ! $SKIP_DASHBOARD && [[ -f "$DASHBOARD_JSON" ]] && [[ -n "$WAREHOUSE_ID" ]]; then
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
      STATE_FILE="$ROOT/.deploy-state.json"
      if [[ -f "$STATE_FILE" ]]; then
        python3 -c "
import json
with open('$STATE_FILE') as f: d = json.load(f)
d['dashboard_id'] = '$DASHBOARD_ID'
d['workspace_path'] = '$WORKSPACE_PATH'
with open('$STATE_FILE', 'w') as f: json.dump(d, f)
"
      else
        echo "{\"dashboard_id\": \"$DASHBOARD_ID\", \"workspace_path\": \"$WORKSPACE_PATH\"}" > "$STATE_FILE"
      fi
    fi
  else
    echo "Note: Dashboard create failed. Create manually from dashboards/Retail_Bank_Fraud_Dashboard.lvdash.json (Import dashboard in the UI) if needed."
  fi
elif ! $SKIP_DASHBOARD && [[ ! -f "$DASHBOARD_JSON" ]]; then
  echo "Warning: Dashboard template not found at $DASHBOARD_JSON; skipping dashboard."
fi

# --- Genie space: create space for gold_transactions only ---
if $GENIE; then
  echo "=== Creating Genie space (gold_transactions) ==="
  GENIE_REQ_TMP="$(mktemp)"
  python3 <<PY
import json
# Minimal serialized_space: one view only (gold_transactions)
space = {
  "version": 2,
  "data_sources": {
    "tables": [
      {
        "identifier": "$CATALOG.$SCHEMA.gold_transactions",
        "description": ["Gold transactions view for retail bank fraud analytics"]
      }
    ]
  }
}
req = {
  "title": "Retail Bank Fraud Genie",
  "description": "Genie space for gold_transactions view",
  "warehouse_id": "$WAREHOUSE_ID",
  "serialized_space": json.dumps(space)
}
with open("$GENIE_REQ_TMP", "w") as f:
    json.dump(req, f)
PY
  GENIE_OUT="$(dbx api post /api/2.0/genie/spaces --json @"$GENIE_REQ_TMP" -o json 2>/dev/null)" || true
  rm -f "$GENIE_REQ_TMP"
  if [[ -n "$GENIE_OUT" ]]; then
    GENIE_SPACE_ID="$(echo "$GENIE_OUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('space_id',''))" 2>/dev/null)"
    if [[ -n "$GENIE_SPACE_ID" ]]; then
      echo "Genie space created: $GENIE_SPACE_ID"
      STATE_FILE="$ROOT/.deploy-state.json"
      if [[ -f "$STATE_FILE" ]]; then
        python3 <<PY
import json
with open("$STATE_FILE") as f:
    d = json.load(f)
d["genie_space_id"] = "$GENIE_SPACE_ID"
with open("$STATE_FILE", "w") as f:
    json.dump(d, f)
PY
      else
        echo "{\"genie_space_id\": \"$GENIE_SPACE_ID\", \"workspace_path\": \"$WORKSPACE_PATH\"}" > "$STATE_FILE"
      fi
    fi
  else
    echo "Note: Genie space create failed. Check warehouse_id and API access."
  fi
fi

echo "=== deploy.sh finished ==="
