#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source "$ROOT/scripts/runtime.sh"
alp_load_env "$ROOT"

HOST="${PURPOSE_FIELD_HOST:-127.0.0.1}"
PORT="${PURPOSE_FIELD_PORT:-8844}"
BASE="http://$HOST:$PORT"
TMPDIR_VERIFY="$(mktemp -d "${TMPDIR:-/tmp}/alp-verify.XXXXXX")"
trap 'rm -rf "$TMPDIR_VERIFY"' EXIT

QUERY='{"query":"An adult wants their own apartment, budgeting skills, reliable transportation, community participation, and a path to paid employment.","mode":"person","perspective":"case_manager","limit":8}'

echo "A LIVING PURPOSE — PURPOSE FIELD VERIFY"
echo "────────────────────────────────────────────────────────"

echo
echo "1) Built SQLite field + live ARBITER"
python3 "$ROOT/app.py" verify

echo
echo "2) Running service identity"
CODE="$(curl -sS -o "$TMPDIR_VERIFY/health.json" -w '%{http_code}' "$BASE/health" || true)"
if [ "$CODE" != "200" ]; then
  echo "Health request failed · HTTP ${CODE:-none}"
  cat "$TMPDIR_VERIFY/health.json" 2>/dev/null || true
  exit 1
fi
alp_assert_health_file "$TMPDIR_VERIFY/health.json"
python3 -m json.tool "$TMPDIR_VERIFY/health.json"

echo
echo "3) HTTP semantic search"
alp_request_json POST "$BASE/api/search" "$QUERY" "$TMPDIR_VERIFY/search.json"
python3 - "$TMPDIR_VERIFY/search.json" <<'PY_SEARCH'
import json
import sys

d = json.load(open(sys.argv[1], encoding="utf-8"))
rows = d.get("results") or []
if not rows:
    raise SystemExit("semantic search returned no results")
if d.get("embedding_source") != "arbiter":
    raise SystemExit(f"unexpected embedding source: {d.get('embedding_source')!r}")
print(f"SEARCH PASS · {len(rows)} results · {d.get('elapsed_ms')}ms · {d.get('embedding_source')}")
for index, row in enumerate(rows[:5], 1):
    print(f"  {index:02d} {float(row.get('score', 0)):.3f} · {row.get('category')} · {row.get('organization')} · {row.get('title')}")
PY_SEARCH

echo
echo "VERIFY COMPLETE · FULL ECOSYSTEM FIELD"
