#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

"$ROOT/scripts/START_BACKGROUND.command"
echo
"$ROOT/scripts/BUILD_FIELD.command"
echo
"$ROOT/scripts/VERIFY.command"
echo
open "http://127.0.0.1:${PURPOSE_FIELD_PORT:-8844}" 2>/dev/null || true
