#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

set -a
[ -f "$ROOT/.env" ] && source "$ROOT/.env"
set +a
export ARBITER_EMBED_URL="${ARBITER_EMBED_URL:-https://api.arbiter.traut.ai/public/embed}"

echo "A LIVING PURPOSE — BUILD THE FULL SUPPORT FIELD"
echo "────────────────────────────────────────────────────────"
echo "This crawls the registered official sources, chunks their pages,"
echo "embeds every record through ARBITER, and replaces the previous field."
echo
python3 "$ROOT/app.py" build --reset
