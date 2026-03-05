#!/usr/bin/env bash
# Quick-start deployment script: authenticate, push data to UC volume, create tables/views.
# Supports --non-interactive (one-shot for CI/agents) and interactive prompting.
# Usage: ./create.sh [OPTIONS]; see --help.

set -e

# --- Repo root ---
ROOT="$(cd "$(dirname "$0")" && git rev-parse --show-toplevel 2>/dev/null)" || ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

# --- Defaults ---
PROFILE=""
HOST=""
CATALOG=""
SCHEMA=""
VOLUME=""
DATA_DIR="$ROOT/data"
WORKSPACE_PATH=""
NON_INTERACTIVE=false
SKIP_DATA=false
CREATE_VOLUME=false

usage() {
  cat <<'EOF'
Usage: create.sh [OPTIONS]

  -p, --profile PROFILE       Databricks CLI profile name (from ~/.databrickscfg)
  --host HOST                 Workspace URL (e.g. https://xxx.cloud.databricks.com)
  -c, --catalog CATALOG       Unity Catalog catalog name
  -s, --schema SCHEMA         Unity Catalog schema name
  -v, --volume VOLUME         UC volume name for data (Volumes/<catalog>/<schema>/<volume>/retail/...)
  --data-dir DIR              Local data directory (default: ./data)
  --workspace-path PATH       Target workspace path for notebooks (e.g. /Users/you@example.com/dbx-bank-fraud)
  -y, --yes, --non-interactive  No prompts; fail if auth or required params missing
  -i, --interactive           Explicit interactive mode (prompt for missing params)
  --skip-data                 Do not copy files to volume; assume data already there
  --create-volume             Create the UC volume if it does not exist (MANAGED)
  -h, --help                  Show this help and exit

Examples:
  ./create.sh --non-interactive -p myprofile -c main -s default -v bank-fraud
  ./create.sh
EOF
}

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--profile)    PROFILE="$2"; shift 2 ;;
    --host)          HOST="$2"; shift 2 ;;
    -c|--catalog)    CATALOG="$2"; shift 2 ;;
    -s|--schema)     SCHEMA="$2"; shift 2 ;;
    -v|--volume)     VOLUME="$2"; shift 2 ;;
    --data-dir)      DATA_DIR="$2"; shift 2 ;;
    --workspace-path) WORKSPACE_PATH="$2"; shift 2 ;;
    -y|--yes|--non-interactive) NON_INTERACTIVE=true; shift ;;
    -i|--interactive) NON_INTERACTIVE=false; shift ;;
    --skip-data)    SKIP_DATA=true; shift ;;
    --create-volume) CREATE_VOLUME=true; shift ;;
    -h|--help)      usage; exit 0 ;;
    *)              echo "Unknown option: $1"; usage; exit 1 ;;
  esac
done

# --- Helpers ---
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
  prompt="$prompt: "
  read -r -p "$prompt" input
  if [[ -z "$input" && -n "$default" ]]; then
    input="$default"
  fi
  printf -v "$var_ref" '%s' "$input"
}

# --- Auth: list profiles ---
echo "=== Databricks CLI profiles ==="
if ! databricks auth profiles 2>/dev/null; then
  echo "Could not list profiles. Is the Databricks CLI installed? (pip install databricks-cli or databricks CLI)"
  exit 1
fi

# --- Resolve profile (interactive or non-interactive) ---
if $NON_INTERACTIVE; then
  if [[ -z "$PROFILE" && -z "$HOST" ]]; then
    echo "Error: In non-interactive mode either --profile or --host is required."
    usage
    exit 1
  fi
  if [[ -z "$PROFILE" && -n "$HOST" ]]; then
    echo "Logging in with host: $HOST"
    databricks auth login --host "$HOST" || true
    PROFILE="DEFAULT"
  fi
  if [[ -z "$PROFILE" ]]; then
    PROFILE="DEFAULT"
  fi
else
  if [[ -z "$PROFILE" ]]; then
    echo ""
    read -r -p "Enter profile name (from list above) or press Enter to login with host: " prof_in
    if [[ -n "$prof_in" ]]; then
      PROFILE="$prof_in"
    else
      read -r -p "Workspace URL (e.g. https://xxx.cloud.databricks.com): " HOST
      [[ -z "$HOST" ]] && { echo "Host required."; exit 1; }
      databricks auth login --host "$HOST" || true
      PROFILE="DEFAULT"
    fi
  fi
fi

# --- Verify auth ---
echo "Verifying auth for profile: $PROFILE"
if ! dbx workspace list / &>/dev/null; then
  if $NON_INTERACTIVE; then
    echo "Error: Auth verification failed. Run: databricks auth login -p $PROFILE"
    exit 1
  fi
  echo "Auth check failed. Please run: databricks auth login -p $PROFILE"
  read -r -p "Retry after login? [y/N]: " retry
  if [[ "${retry,,}" != "y" && "${retry,,}" != "yes" ]]; then
    exit 1
  fi
  if ! dbx workspace list / &>/dev/null; then
    echo "Verification still failed."
    exit 1
  fi
fi
export DATABRICKS_CONFIG_PROFILE="$PROFILE"
echo "Using profile: $PROFILE"

# --- Resolve catalog, schema, volume (prompt if missing and interactive) ---
while ! require_param "catalog" "$CATALOG"; do prompt_val "Unity Catalog name" "" CATALOG; done
while ! require_param "schema" "$SCHEMA"; do prompt_val "Unity Schema name" "" SCHEMA; done
while ! require_param "volume" "$VOLUME"; do prompt_val "Volume name" "" VOLUME; done

VOLUME_BASE="dbfs:/Volumes/$CATALOG/$SCHEMA/$VOLUME"
VOLUME_RETAIL="$VOLUME_BASE/retail"

# --- Create volume if requested ---
if $CREATE_VOLUME; then
  echo "=== Creating volume (if not exists) ==="
  if dbx volumes create "$CATALOG" "$SCHEMA" "$VOLUME" MANAGED 2>/dev/null; then
    echo "Volume created."
  else
    echo "Volume may already exist; continuing."
  fi
fi

# --- Create directory structure in volume ---
echo "=== Creating volume directory structure ==="
for dir in retail retail/transactions retail/fraud_reports retail/customers retail/country_code; do
  dbx fs mkdir "$VOLUME_BASE/$dir" 2>/dev/null || true
done

# --- Copy data (unless --skip-data) ---
if ! $SKIP_DATA; then
  echo "=== Copying data to volume ==="
  copy_one() {
    local src="$1"
    local dest="$2"
    local recursive="${3:-false}"
    if [[ ! -e "$src" ]]; then
      echo "Warning: Source missing, skipping: $src"
      return 0
    fi
    if $recursive; then
      dbx fs cp "$src" "$dest" --overwrite --recursive
    else
      dbx fs cp "$src" "$dest" --overwrite
    fi
  }
  copy_one "$DATA_DIR/transactions"           "$VOLUME_RETAIL/transactions/" true
  copy_one "$DATA_DIR/fraud_reports"           "$VOLUME_RETAIL/fraud_reports/" true
  copy_one "$DATA_DIR/customers_json"          "$VOLUME_RETAIL/customers/"     true
  copy_one "$DATA_DIR/country_coordinates/country_coordinates.csv" "$VOLUME_RETAIL/country_code/" false
else
  echo "=== Skipping data copy (--skip-data) ==="
fi

# --- ETL: create tables and view ---
echo "=== Creating tables and view ==="
python etl/create.py -c "$CATALOG" -s "$SCHEMA" -t bronze_transactions -v "$VOLUME"
python etl/create.py -c "$CATALOG" -s "$SCHEMA" -t fraud_reports -v "$VOLUME"
python etl/create.py -c "$CATALOG" -s "$SCHEMA" -t banking_customers -v "$VOLUME" -f customers --format json
python etl/create.py -c "$CATALOG" -s "$SCHEMA" -t country_coordinates -v "$VOLUME" -f country_code
python etl/create.py -c "$CATALOG" -s "$SCHEMA" -t silver_transactions -v "$VOLUME"
python etl/create.py -c "$CATALOG" -s "$SCHEMA" -t gold_transactions -v "$VOLUME"

# --- Optional: import notebooks to workspace ---
if [[ -n "$WORKSPACE_PATH" ]]; then
  echo "=== Importing notebooks to workspace ==="
  if [[ -d "$ROOT/notebooks" ]]; then
    dbx workspace import-dir "$ROOT/notebooks" "$WORKSPACE_PATH/notebooks" -o
  fi
  if [[ -d "$ROOT/jobs" ]]; then
    dbx workspace import-dir "$ROOT/jobs" "$WORKSPACE_PATH/jobs" -o
  fi
  echo "Imported to $WORKSPACE_PATH"
fi

echo "=== create.sh finished successfully ==="
