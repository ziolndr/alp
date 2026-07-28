#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

set -a
[ -f "$ROOT/.env" ] && source "$ROOT/.env"
set +a
export ARBITER_EMBED_URL="${ARBITER_EMBED_URL:-https://api.arbiter.traut.ai/public/embed}"

echo "A LIVING PURPOSE — BOOTSTRAP SOURCE FIELD"
echo "────────────────────────────────────────────────────────"
echo "Embedding the 64 official source profiles without crawling pages."
echo "Use BUILD_FIELD.command afterward for the full live corpus."
echo
python3 "$ROOT/app.py" build --reset --metadata-only
