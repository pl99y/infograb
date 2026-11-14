#!/usr/bin/env bash
set -Eeuo pipefail

job=$1
case "$job" in
  15m) exporter="scripts/export_public_json.py" ;;
  12h) exporter="scripts/export_public_json_12h.py" ;;
  *) printf 'Unknown dashboard profile: %s\n' "$job" >&2; exit 2 ;;
esac

app_home="${INFOGRAB_APP_HOME:-/home/opc/infograb}"
repo="$app_home/repo"
log_dir="$app_home/logs"
log_file="$log_dir/run_$job.log"
deploy_dir=""
max_log_bytes=$((2 * 1024 * 1024))

cleanup() {
  if [[ -n "$deploy_dir" ]]; then
    rm -rf -- "$deploy_dir"
  fi
}

on_error() {
  printf '[error] run_%s failed at line %s with exit code %s\n' "$job" "$1" "$2"
}

mkdir -p "$log_dir"
# Both profiles share the source checkout, database and publication branch.
exec 9>"$app_home/infograb.lock"
flock 9

if [[ -f "$log_file" ]] && (( $(wc -c < "$log_file") > max_log_bytes )); then
  rotated="$log_file.$(date -u +%Y%m%dT%H%M%SZ)"
  mv -- "$log_file" "$rotated"
  gzip -f -- "$rotated" || true
fi
# Keep at most four rotated files per job, in addition to the active log.
for profile in 15m 12h; do
  archives=("$log_dir"/run_"$profile".log.[0-9]*Z*)
  kept=0
  for ((index=${#archives[@]} - 1; index >= 0; index--)); do
    archive=${archives[index]}
    [[ -f "$archive" ]] || continue
    if (( kept >= 4 )); then
      rm -f -- "$archive"
    else
      kept=$((kept + 1))
    fi
  done
done
find "$log_dir" -maxdepth 1 -type f -name 'run_*.log.[0-9]*Z*' -mmin +10080 -delete
# A killed publish may leave its temporary checkout. The lock makes these
# inactive, and the age cutoff leaves recent failures available to inspect.
find "$app_home" -maxdepth 1 -type d -name 'deploy-gh-pages.??????' -mmin +1440 \
  -exec rm -rf -- {} +
exec >> "$log_file" 2>&1
trap cleanup EXIT
trap 'on_error "$LINENO" "$?"' ERR

echo "===== $(date -u +%FT%TZ) run_$job start ====="
cd "$repo"
if [[ "$(git symbolic-ref --quiet --short HEAD)" != main ]] ||
   ! git diff --quiet || ! git diff --cached --quiet; then
  echo "[error] the source checkout must be clean and on main"
  git status --short --branch
  exit 1
fi
git pull --ff-only origin main

source "$app_home/venv/bin/activate"
set -a
source .env
set +a

origin=$(git config --get remote.origin.url)
pages_ref=refs/heads/gh-pages
base=""
if remote_ref=$(git ls-remote --exit-code --heads origin "$pages_ref"); then
  read -r base _ <<< "$remote_ref"
  [[ -n "$base" ]]
else
  status=$?
  if (( status != 2 )); then
    exit "$status"
  fi
fi

deploy_dir=$(mktemp -d "$app_home/deploy-gh-pages.XXXXXX")
git -C "$deploy_dir" init -q -b gh-pages
git -C "$deploy_dir" remote add origin "$origin"
git archive HEAD:docs | tar -xf - -C "$deploy_dir"
if [[ -n "$base" ]]; then
  git -C "$deploy_dir" fetch -q --depth=1 origin "$base"
  if git -C "$deploy_dir" cat-file -e "$base:data" 2>/dev/null; then
    git -C "$deploy_dir" archive "$base" data | tar -xf - -C "$deploy_dir"
    echo "[ok] hydrated previous data from gh-pages $base"
  fi
fi

INFOGRAB_DATA_DIR="$deploy_dir/data" python "$exporter"
test -f "$deploy_dir/index.html"
test -f "$deploy_dir/data/export_meta.json"
touch "$deploy_dir/.nojekyll"

git_user_name=$(git config user.name || true)
git_user_email=$(git config user.email || true)
git -C "$deploy_dir" add -A
git -C "$deploy_dir" \
  -c user.name="${git_user_name:-infograb-bot}" \
  -c user.email="${git_user_email:-infograb-bot@users.noreply.github.com}" \
  commit -q -m "Deploy dashboard snapshot ($job)"

unchanged=false
if [[ -n "$base" ]]; then
  if git -C "$deploy_dir" diff --quiet "$base" HEAD --; then
    unchanged=true
  else
    status=$?
    if (( status != 1 )); then
      exit "$status"
    fi
  fi
fi
if [[ "$unchanged" == true ]]; then
  echo "[ok] no gh-pages deploy changes"
else
  # Use the same commit for hydration and the push lease. An empty base requires
  # the branch to still be absent, so initial publication is protected too.
  git -C "$deploy_dir" push --force-with-lease="$pages_ref:$base" origin "HEAD:$pages_ref"
  echo "[ok] deployed docs snapshot to gh-pages"
fi

git gc --auto || true
echo "===== $(date -u +%FT%TZ) run_$job done ====="
