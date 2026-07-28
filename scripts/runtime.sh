#!/bin/bash
# Shared runtime functions for Geneva Purpose Field.
# shellcheck shell=bash

alp_load_env() {
  local root="$1"
  set -a
  [ -f "$root/.env" ] && source "$root/.env"
  set +a
  export ARBITER_EMBED_URL="${ARBITER_EMBED_URL:-https://api.arbiter.traut.ai/public/embed}"
}

alp_listener_pids() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
  elif command -v fuser >/dev/null 2>&1; then
    fuser -n tcp "$port" 2>/dev/null | tr ' ' '\n' | grep -E '^[0-9]+$' || true
  fi
}

alp_reclaim_port() {
  local port="$1"
  local pids pid
  pids="$(alp_listener_pids "$port" | tr '\n' ' ')"
  [ -z "${pids// /}" ] && return 0

  echo "Port $port is occupied; stopping stale listener(s): $pids"
  for pid in $pids; do
    ps -p "$pid" -o pid=,command= 2>/dev/null || true
    kill "$pid" 2>/dev/null || true
  done

  for _ in $(seq 1 30); do
    [ -z "$(alp_listener_pids "$port")" ] && return 0
    sleep .2
  done

  pids="$(alp_listener_pids "$port" | tr '\n' ' ')"
  if [ -n "${pids// /}" ]; then
    echo "Forcing stale listener(s) off port $port: $pids"
    for pid in $pids; do
      kill -9 "$pid" 2>/dev/null || true
    done
  fi

  for _ in $(seq 1 20); do
    [ -z "$(alp_listener_pids "$port")" ] && return 0
    sleep .2
  done

  echo "Could not reclaim port $port."
  return 1
}

alp_assert_health_file() {
  local file="$1"
  python3 - "$file" <<'PY_HEALTH'
import json
import sys

path = sys.argv[1]
try:
    data = json.load(open(path, encoding="utf-8"))
except Exception as exc:
    print(f"invalid health response: {exc}", file=sys.stderr)
    raise SystemExit(1)

if data.get("service") != "Geneva — Purpose Field":
    print(f"wrong service on port: {data.get('service')!r}", file=sys.stderr)
    raise SystemExit(1)
if data.get("architecture") != "ecosystem-field":
    print(f"wrong architecture on port: {data.get('architecture')!r}", file=sys.stderr)
    raise SystemExit(1)
if not str(data.get("version") or "").startswith("1."):
    print(f"wrong Purpose Field version: {data.get('version')!r}", file=sys.stderr)
    raise SystemExit(1)
if int(data.get("sources") or 0) < 67:
    print(f"source registry incomplete: {data.get('sources')!r}", file=sys.stderr)
    raise SystemExit(1)
PY_HEALTH
}

alp_request_json() {
  local method="$1"
  local url="$2"
  local body="${3:-}"
  local output="$4"
  local code

  if [ "$method" = "GET" ]; then
    code="$(curl -sS -o "$output" -w '%{http_code}' "$url" || true)"
  else
    code="$(curl -sS -o "$output" -w '%{http_code}' -X "$method" \
      -H 'Content-Type: application/json' --data "$body" "$url" || true)"
  fi

  if [ "$code" != "200" ] && [ "$code" != "202" ]; then
    echo "HTTP request failed · $method $url · status ${code:-none}" >&2
    cat "$output" >&2 2>/dev/null || true
    return 1
  fi

  python3 -m json.tool "$output"
}
