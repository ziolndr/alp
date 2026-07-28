#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/runtime.sh"
alp_load_env "$ROOT"
HOST="${PURPOSE_FIELD_HOST:-127.0.0.1}"
PORT="${PURPOSE_FIELD_PORT:-8844}"
TMP="$(mktemp "${TMPDIR:-/tmp}/alp-status.XXXXXX")"
trap 'rm -f "$TMP"' EXIT
alp_request_json GET "http://$HOST:$PORT/api/stats" '' "$TMP"
