#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
set -a
[ -f "$ROOT/.env" ] && source "$ROOT/.env"
set +a
export ARBITER_EMBED_URL="${ARBITER_EMBED_URL:-https://api.arbiter.traut.ai/public/embed}"
python3 "$ROOT/app.py" probe
