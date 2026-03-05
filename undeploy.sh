#!/usr/bin/env bash
# Undeploy artefacts created by deploy.sh: trash the dashboard, optionally remove workspace notebooks.
# Usage: ./undeploy.sh [OPTIONS]; see --help.

set -e

ROOT="$(cd "$(dirname "$0")" && git rev-parse --show-toplevel 2>/dev/null)" || ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

PROFILE=""
HOST=""
REMOVE_WORKSPACE=false
NON_INTERACTIVE=false
STATE_FILE="$ROOT/.deploy-state.json"

usage() {
  cat <<'EOF'
Usage: undeploy.sh [OPTIONS]

  -p, --profile PROFILE       Databricks CLI profile name
  --host HOST                 Workspace URL
  --remove-workspace          Also delete the deployed notebooks from the workspace path (from .deploy-state.json)
  -y, --yes, --non-interactive  No prompts
  -i, --interactive           Prompt for missing params
  -h, --help                  Show this help and exit

Reads .deploy-state.json (written by deploy.sh) to find the dashboard ID to trash.
Does not unregister ML models or delete experiments.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--profile)       PROFILE="$2"; shift 2 ;;
    --host)             HOST="$2"; shift 2 ;;
    --remove-workspace) REMOVE_WORKSPACE=true; shift ;;
    -y|--yes|--non-interactive) NON_INTERACTIVE=true; shift ;;
    -i|--interactive)  NON_INTERACTIVE=false; shift ;;
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

# --- Trash dashboard from state ---
if [[ -f "$STATE_FILE" ]]; then
  DASHBOARD_ID="$(python3 -c "import json; d=json.load(open('$STATE_FILE')); print(d.get('dashboard_id',''))" 2>/dev/null)"
  WORKSPACE_PATH="$(python3 -c "import json; d=json.load(open('$STATE_FILE')); print(d.get('workspace_path',''))" 2>/dev/null)"
  if [[ -n "$DASHBOARD_ID" ]]; then
    echo "=== Trashing dashboard $DASHBOARD_ID ==="
    dbx lakeview trash "$DASHBOARD_ID" 2>/dev/null || true
    echo "Dashboard trashed."
  else
    echo "No dashboard_id in $STATE_FILE; skipping dashboard."
  fi
else
  echo "No .deploy-state.json found (run deploy.sh first). Skipping dashboard."
  WORKSPACE_PATH=""
fi

# --- Optionally remove workspace path ---
if $REMOVE_WORKSPACE && [[ -n "$WORKSPACE_PATH" ]]; then
  echo "=== Deleting workspace path: $WORKSPACE_PATH ==="
  dbx workspace delete -r "$WORKSPACE_PATH" 2>/dev/null || true
  echo "Workspace path removed."
elif $REMOVE_WORKSPACE && [[ -z "$WORKSPACE_PATH" ]]; then
  echo "Cannot --remove-workspace: no workspace_path in .deploy-state.json."
fi

# Clean state file so re-deploy creates a fresh dashboard
[[ -f "$STATE_FILE" ]] && rm -f "$STATE_FILE"

echo "=== undeploy.sh finished ==="
