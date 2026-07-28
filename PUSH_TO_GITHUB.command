#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
REMOTE="https://github.com/ziolndr/alp.git"

command -v git >/dev/null 2>&1 || { echo "git is required"; exit 1; }

if [ ! -d .git ]; then
  git init
fi
git branch -M main

if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$REMOTE"
else
  git remote add origin "$REMOTE"
fi

git add -A
if ! git diff --cached --quiet; then
  git commit -m "Anchor Purpose Field around A Living Purpose services"
else
  echo "No new local changes to commit."
fi

echo "Pushing to $REMOTE"
git push -u origin main

echo "A LIVING PURPOSE FIELD PUSHED"
