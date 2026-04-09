#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="/home/zhangzhanyi/workspace/janus_vol_pred"
PACKAGE_ROOT="$PROJECT_ROOT/janus_vol_pred"
LOG_DIR="$PACKAGE_ROOT/logs/daily_jobs"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"

mkdir -p "$LOG_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing virtualenv python: $PYTHON_BIN" >&2
  exit 1
fi

cd "$PROJECT_ROOT"

STAMP="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="$LOG_DIR/daily_production_${STAMP}.log"

"$PYTHON_BIN" "$PACKAGE_ROOT/daily_production_job.py" >> "$LOG_FILE" 2>&1
