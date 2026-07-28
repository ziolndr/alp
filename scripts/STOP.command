#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/runtime.sh"
alp_load_env "$ROOT"
PORT="${PURPOSE_FIELD_PORT:-8844}"
PIDFILE="$ROOT/logs/purpose-field.pid"

if [ -f "$PIDFILE" ]; then
  PID="$(cat "$PIDFILE" 2>/dev/null || true)"
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    kill "$PID" 2>/dev/null || true
  fi
fi
alp_reclaim_port "$PORT"
rm -f "$PIDFILE"
echo "PURPOSE FIELD STOPPED · port $PORT clear"
