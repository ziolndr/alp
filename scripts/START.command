#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/runtime.sh"
alp_load_env "$ROOT"
HOST="${PURPOSE_FIELD_HOST:-127.0.0.1}"
PORT="${PURPOSE_FIELD_PORT:-8844}"
alp_reclaim_port "$PORT"

echo "A LIVING PURPOSE — PURPOSE FIELD"
echo "────────────────────────────────────────────────────────"
echo "ARBITER · $ARBITER_EMBED_URL"
echo "APP · http://$HOST:$PORT"
echo "SOURCES · $(python3 - <<'PY_SOURCE_COUNT'
import json
print(len(json.load(open('data/sources.json', encoding='utf-8'))['sources']))
PY_SOURCE_COUNT
)"
echo
exec python3 "$ROOT/app.py" serve
