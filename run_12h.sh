#!/usr/bin/env bash
set -Eeuo pipefail

APP_HOME="/home/opc/infograb"
REPO_DIR="$APP_HOME/repo"
VENV_ACTIVATE="$APP_HOME/venv/bin/activate"
LOG_DIR="$APP_HOME/logs"
LOG_FILE="$LOG_DIR/run_12h.log"
LOCK_FILE="$APP_HOME/infograb.lock"
MAX_LOG_BYTES=$((2 * 1024 * 1024))
KEEP_LOG_DAYS=7
JOB_LABEL="12h"
PYTHON_EXPORT_SCRIPT="scripts/export_public_json_12h.py"
DEPLOY_TMP=""

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

cleanup_tmp() {
  if [[ -n "${DEPLOY_TMP:-}" && -d "$DEPLOY_TMP" ]]; then
    rm -rf "$DEPLOY_TMP"
  fi
}

on_error() {
  local code=$?
  echo "[error] run_${JOB_LABEL} failed at line $LINENO with exit code $code"
  exit "$code"
}

rotate_log_if_needed "$LOG_FILE"
cleanup_old_logs
exec >> "$LOG_FILE" 2>&1

trap cleanup_tmp EXIT
trap on_error ERR

# Prevent the 15m and 12h jobs from modifying the repo/db at the same time.
exec 9>"$LOCK_FILE"
flock 9

ensure_clean_source_tree() {
  cd "$REPO_DIR"
  if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "[error] source tree has uncommitted changes. Commit, stash, or reset them before running."
    git status --short
    exit 1
  fi
}

restore_or_remove_generated_data() {
  cd "$REPO_DIR"

  # If docs/data is still tracked on main, restore it so the source branch stays clean.
  # If it has already been untracked and ignored, remove the generated local copy.
  if git ls-files --error-unmatch docs/data >/dev/null 2>&1; then
    git restore --staged docs/data 2>/dev/null || true
    git restore docs/data 2>/dev/null || true
    git clean -fd docs/data 2>/dev/null || true
  else
    rm -rf docs/data
  fi
}

deploy_docs_to_gh_pages() {
  cd "$REPO_DIR"

  if [[ ! -f "$REPO_DIR/docs/index.html" ]]; then
    echo "[error] docs/index.html not found; refusing to deploy"
    exit 1
  fi
  if [[ ! -d "$REPO_DIR/docs/data" ]]; then
    echo "[error] docs/data not found after export; refusing to deploy"
    exit 1
  fi

  local remote_url
  remote_url=$(git config --get remote.origin.url)
  if [[ -z "$remote_url" ]]; then
    echo "[error] git remote origin is not configured"
    exit 1
  fi

  DEPLOY_TMP=$(mktemp -d "$APP_HOME/deploy-gh-pages.XXXXXX")
  cp -a "$REPO_DIR/docs/." "$DEPLOY_TMP/"
  touch "$DEPLOY_TMP/.nojekyll"

  cd "$DEPLOY_TMP"
  git init -q
  git checkout -q -b gh-pages

  local git_user_name git_user_email
  git_user_name=$(git -C "$REPO_DIR" config user.name || true)
  git_user_email=$(git -C "$REPO_DIR" config user.email || true)
  [[ -n "$git_user_name" ]] || git_user_name="infograb-bot"
  [[ -n "$git_user_email" ]] || git_user_email="infograb-bot@users.noreply.github.com"
  git config user.name "$git_user_name"
  git config user.email "$git_user_email"

  git remote add origin "$remote_url"
  git add -A
  git commit -q -m "Deploy dashboard snapshot (${JOB_LABEL})"

  if git ls-remote --exit-code --heads origin gh-pages >/dev/null 2>&1; then
    git fetch -q --depth=1 origin gh-pages:refs/remotes/origin/gh-pages

    if git diff --quiet HEAD refs/remotes/origin/gh-pages --; then
      echo "[ok] no gh-pages deploy changes"
      return 0
    fi

    local remote_sha
    remote_sha=$(git rev-parse refs/remotes/origin/gh-pages)
    git push --force-with-lease="refs/heads/gh-pages:${remote_sha}" origin HEAD:refs/heads/gh-pages
  else
    git push -u origin HEAD:refs/heads/gh-pages
  fi

  echo "[ok] deployed docs snapshot to gh-pages"
}

echo "===== $(date -u '+%Y-%m-%dT%H:%M:%SZ') run_${JOB_LABEL} start ====="

cd "$REPO_DIR"
ensure_clean_source_tree
git pull --ff-only origin main

source "$VENV_ACTIVATE"
set -a
source .env
set +a

python "$PYTHON_EXPORT_SCRIPT"
deploy_docs_to_gh_pages
restore_or_remove_generated_data

# Keep the source branch clean. This should not create commits on main.
ensure_clean_source_tree

git gc --auto || true

echo "===== $(date -u '+%Y-%m-%dT%H:%M:%SZ') run_${JOB_LABEL} done ====="

