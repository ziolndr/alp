#!/bin/bash
set -e
ROOT="$(cd "$(dirname "$0")" && pwd)"
exec "$ROOT/scripts/START_AND_BUILD.command"
