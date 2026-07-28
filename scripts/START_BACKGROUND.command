#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/runtime.sh"
alp_load_env "$ROOT"

HOST="${PURPOSE_FIELD_HOST:-127.0.0.1}"
PORT="${PURPOSE_FIELD_PORT:-8844}"
mkdir -p "$ROOT/logs" "$ROOT/data"
LOG="$ROOT/logs/purpose-field.log"
PIDFILE="$ROOT/logs/purpose-field.pid"
HEALTH_FILE="$ROOT/logs/start-health.json"

# Port 8844 belongs to this product. Remove any stale prototype first.
alp_reclaim_port "$PORT"
rm -f "$PIDFILE" "$HEALTH_FILE"

nohup python3 "$ROOT/app.py" serve >"$LOG" 2>&1 &
PID=$!
echo "$PID" > "$PIDFILE"

for _ in $(seq 1 80); do
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "Purpose Field process exited during startup."
    tail -120 "$LOG" || true
    exit 1
  fi

  CODE="$(curl -sS -o "$HEALTH_FILE" -w '%{http_code}' "http://$HOST:$PORT/health" 2>/dev/null || true)"
  if [ "$CODE" = "200" ] && alp_assert_health_file "$HEALTH_FILE" >/dev/null 2>&1; then
    echo "PURPOSE FIELD READY · http://$HOST:$PORT"
    echo "PROCESS · $PID"
    echo "ARBITER · $ARBITER_EMBED_URL"
    python3 - "$HEALTH_FILE" <<'PY_READY'
import json
import sys

d = json.load(open(sys.argv[1], encoding="utf-8"))
print(f"FIELD · {d.get('records', 0):,} records · {d.get('ready_sources', 0)}/{d.get('sources', 0)} sources · {d.get('vector_dimensions')}D")
print(f"IDENTITY · {d.get('architecture')} · v{d.get('version')}")
PY_READY
    echo "LOG · $LOG"
    exit 0
  fi
  sleep .25
done

echo "Purpose Field did not become ready with the expected ecosystem-field identity."
[ -f "$HEALTH_FILE" ] && cat "$HEALTH_FILE" || true
tail -120 "$LOG" || true
kill "$PID" 2>/dev/null || true
rm -f "$PIDFILE"
exit 1
