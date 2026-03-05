#!/usr/bin/env bash
# Quick-start teardown: drop tables/views, remove volume data, optionally delete workspace path.
# Supports --non-interactive and interactive modes.
# Usage: ./destroy.sh [OPTIONS]; see --help.

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
WORKSPACE_PATH=""
NON_INTERACTIVE=false

usage() {
  cat <<'EOF'
Usage: destroy.sh [OPTIONS]

  -p, --profile PROFILE       Databricks CLI profile name (from ~/.databrickscfg)
  --host HOST                 Workspace URL (e.g. https://xxx.cloud.databricks.com)
  -c, --catalog CATALOG       Unity Catalog catalog name
  -s, --schema SCHEMA         Unity Catalog schema name
  -v, --volume VOLUME         UC volume name (same as used in create.sh)
  --workspace-path PATH       Workspace path to delete (e.g. /Users/you@example.com/dbx-bank-fraud)
  -y, --yes, --non-interactive  No prompts; fail if auth or required params missing
  -i, --interactive           Explicit interactive mode (prompt for missing params)
  -h, --help                  Show this help and exit

Examples:
  ./destroy.sh --non-interactive -p myprofile -c main -s default -v bank-fraud
  ./destroy.sh
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
    --workspace-path) WORKSPACE_PATH="$2"; shift 2 ;;
    -y|--yes|--non-interactive) NON_INTERACTIVE=true; shift ;;
    -i|--interactive) NON_INTERACTIVE=false; shift ;;
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
  echo "Could not list profiles. Is the Databricks CLI installed?"
  exit 1
fi

# --- Resolve profile ---
if $NON_INTERACTIVE; then
  if [[ -z "$PROFILE" && -z "$HOST" ]]; then
    echo "Error: In non-interactive mode either --profile or --host is required."
    usage
    exit 1
  fi
  if [[ -z "$PROFILE" && -n "$HOST" ]]; then
    databricks auth login --host "$HOST" || true
    PROFILE="DEFAULT"
  fi
  [[ -z "$PROFILE" ]] && PROFILE="DEFAULT"
else
  if [[ -z "$PROFILE" ]]; then
    echo ""
    read -r -p "Enter profile name or press Enter to login with host: " prof_in
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

# --- Resolve catalog, schema, volume ---
while ! require_param "catalog" "$CATALOG"; do prompt_val "Unity Catalog name" "" CATALOG; done
while ! require_param "schema" "$SCHEMA"; do prompt_val "Unity Schema name" "" SCHEMA; done
while ! require_param "volume" "$VOLUME"; do prompt_val "Volume name" "" VOLUME; done

VOLUME_BASE="dbfs:/Volumes/$CATALOG/$SCHEMA/$VOLUME"
VOLUME_RETAIL="$VOLUME_BASE/retail"

# --- Drop objects (reverse order of create) ---
echo "=== Dropping views and tables ==="
python etl/destroy.py -c "$CATALOG" -s "$SCHEMA" -t gold_transactions
python etl/destroy.py -c "$CATALOG" -s "$SCHEMA" -t silver_transactions
python etl/destroy.py -c "$CATALOG" -s "$SCHEMA" -t bronze_transactions
python etl/destroy.py -c "$CATALOG" -s "$SCHEMA" -t fraud_reports
python etl/destroy.py -c "$CATALOG" -s "$SCHEMA" -t banking_customers
python etl/destroy.py -c "$CATALOG" -s "$SCHEMA" -t country_coordinates

# --- Volume cleanup (remove retail subdirs; volume itself is left in place) ---
echo "=== Removing data from volume ==="
for path in transactions fraud_reports customers country_code; do
  dbx fs rm "$VOLUME_RETAIL/$path" -r 2>/dev/null || true
done

# --- Optional: delete workspace path ---
if [[ -n "$WORKSPACE_PATH" ]]; then
  echo "=== Deleting workspace path: $WORKSPACE_PATH ==="
  dbx workspace delete -r "$WORKSPACE_PATH" 2>/dev/null || true
  echo "Workspace path removed."
fi

echo "=== destroy.sh finished successfully ==="
