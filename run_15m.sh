#!/usr/bin/env bash
set -Eeuo pipefail

APP_HOME="/home/opc/infograb"
REPO_DIR="$APP_HOME/repo"
VENV_ACTIVATE="$APP_HOME/venv/bin/activate"
LOG_DIR="$APP_HOME/logs"
LOG_FILE="$LOG_DIR/run_15m.log"
LOCK_FILE="$APP_HOME/infograb.lock"
MAX_LOG_BYTES=$((2 * 1024 * 1024))
KEEP_LOG_DAYS=7

mkdir -p "$LOG_DIR"

rotate_log_if_needed() {
  local file="$1"
  [[ -f "$file" ]] || return 0

  local size
  size=$(wc -c < "$file" 2>/dev/null || echo 0)
  if (( size > MAX_LOG_BYTES )); then
    local stamp
    stamp=$(date -u +%Y%m%dT%H%M%SZ)
    mv "$file" "$file.$stamp"
    gzip -f "$file.$stamp" || true
  fi
}

cleanup_old_logs() {
  find "$LOG_DIR" -type f \( -name "*.log.*" -o -name "*.log.*.gz" \) -mtime +"$KEEP_LOG_DAYS" -delete 2>/dev/null || true
}

rotate_log_if_needed "$LOG_FILE"
cleanup_old_logs
exec >> "$LOG_FILE" 2>&1

trap 'code=$?; echo "[error] run_15m failed at line $LINENO with exit code $code"; exit $code' ERR

# Prevent the 15m and 12h jobs from modifying the repo/db at the same time.
exec 9>"$LOCK_FILE"
flock 9

echo "===== $(date -u '+%Y-%m-%dT%H:%M:%SZ') run_15m start ====="

cd "$REPO_DIR"
git pull --rebase origin main

source "$VENV_ACTIVATE"
set -a
source .env
set +a

python scripts/export_public_json.py

git add docs/data
if ! git diff --cached --quiet; then
  git commit -m "Update dashboard data (15m)"
  git push origin main
else
  echo "[ok] no docs/data changes"
fi

# This only compresses local git storage when Git thinks it is useful.
# It does not rewrite remote GitHub history.
git gc --auto || true

echo "===== $(date -u '+%Y-%m-%dT%H:%M:%SZ') run_15m done ====="

