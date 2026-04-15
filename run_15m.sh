#!/usr/bin/env bash
set -e

cd /home/opc/infograb/repo
git pull --rebase origin main

source /home/opc/infograb/venv/bin/activate
set -a
source .env
set +a

python scripts/export_public_json.py

git add docs/data
if ! git diff --cached --quiet; then
  git commit -m "Update dashboard data (15m)"
  git push origin main
fi
