#!/usr/bin/env bash
# One allocated disposable proof. This file never selects checkpoints or emits H1.
set -euo pipefail
umask 077
base=045739d08ad4c27211f09f0141375e987df84ae8
image=sha256:bb3e1a57e5407e0a5280b4211980a5e537f4abd234a87014ac979849a78dd825
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"
: "${BIGNAME_RESTORE_RESERVATION:?Explicit serial reservation required}"
: "${BIGNAME_RESTORE_EVIDENCE_DIR:?Absolute isolated evidence directory required}"
: "${BIGNAME_RESTORE_COMMAND_SECS:?Command deadline required}"
: "${BIGNAME_RESTORE_SHUTDOWN_SECS:?Shutdown deadline required}"
: "${BIGNAME_RESTORE_READINESS_SECS:?Readiness deadline required}"
: "${BIGNAME_RESTORE_PROGRESS_SECS:?Progress deadline required}"
: "${BIGNAME_RESTORE_POLL_SECS:?Poll interval required}"
for setting in BIGNAME_RESTORE_COMMAND_SECS BIGNAME_RESTORE_SHUTDOWN_SECS BIGNAME_RESTORE_READINESS_SECS BIGNAME_RESTORE_PROGRESS_SECS BIGNAME_RESTORE_POLL_SECS; do
  [[ ${!setting} =~ ^[1-9][0-9]*$ ]] || { echo "Invalid positive setting: $setting" >&2; exit 1; }
done
for command in docker cargo rustc anvil jq git rg sha256sum timeout setsid cmp od tr ps pkill; do
  command -v "$command" >/dev/null || { echo "Missing required tool: $command" >&2; exit 1; }
done
docker_binary=$(command -v docker)
docker() { timeout --kill-after="$BIGNAME_RESTORE_SHUTDOWN_SECS" "$BIGNAME_RESTORE_COMMAND_SECS" "$docker_binary" "$@"; }
[[ $(git rev-parse HEAD) == "$base" ]] || { echo 'Wrong execution baseline' >&2; exit 1; }
[[ $BIGNAME_RESTORE_EVIDENCE_DIR = /* && ! -e $BIGNAME_RESTORE_EVIDENCE_DIR ]]
mkdir -m 700 "$BIGNAME_RESTORE_EVIDENCE_DIR"
evidence=$BIGNAME_RESTORE_EVIDENCE_DIR
private="$evidence/private"
mkdir -m 700 "$private"
paths=(tests/e2e/src/bin/same_release_restore.rs work/640-same-release-restore/operate.sh work/640-same-release-restore/checkpoint.sql work/640-same-release-restore/roles.sql work/640-same-release-restore/evidence-contract.md tests/e2e/src/harness/pipeline.rs)
caps=(1150 230 650 80 110 10)
allowed() { local entry; for entry in "${paths[@]}"; do [[ $1 == "$entry" ]] && return 0; done; return 1; }
while IFS= read -r changed; do
  [[ -z $changed ]] || allowed "$changed" || { echo "Out-of-scope source: $changed" >&2; exit 1; }
done < <(git diff --name-only "$base"; git ls-files --others --exclude-standard)
while IFS= read -r authored; do
  allowed "$authored" || { echo "Undeclared ignored helper: $authored" >&2; exit 1; }
done < <(find work/640-same-release-restore -type f | LC_ALL=C sort)
total=0
for index in "${!paths[@]}"; do
  path=${paths[$index]}
  [[ -f $path && ! -L $path ]]
  if [[ $index == 5 ]]; then
    gross=$(git diff --numstat "$base" -- "$path" | awk '{n += $1 + $2} END {print n+0}')
  else
    ! git cat-file -e "$base:$path" 2>/dev/null
    gross=$(awk 'END {print NR}' "$path")
  fi
  ((gross <= caps[index])) || { echo "Per-path cap exceeded: $path ($gross)" >&2; exit 1; }
  total=$((total + gross))
  printf '%s\t%s\t%s\n' "$path" "$gross" "${caps[$index]}" >> "$evidence/scope.tsv"
done
((total <= 2230))
printf '%s\n' "$base" > "$evidence/base.txt"
git diff --binary "$base" -- tests/e2e/src/harness/pipeline.rs > "$evidence/accessor.patch"
mkdir "$evidence/authored"
for path in "${paths[@]}"; do cp --parents "$path" "$evidence/authored"; done
sha256sum Cargo.lock tests/e2e/Cargo.lock rust-toolchain.toml > "$evidence/build-inputs.sha256"
ref=.refs/ens_v2
[[ $(git -C "$ref" rev-parse HEAD) == a971bd6449154045e2b26ff13d0e56027452f407 ]]
[[ -z $(git -C "$ref" diff --name-only HEAD -- contracts/deployments/sepolia-20260629-r1) ]]
mkdir "$evidence/artifacts"
for name in .deployment LabelStore RootRegistry ETHRegistry MockUSDC MockDAI StandardRentPriceOracle ETHRegistrar; do
  artifact="$ref/contracts/deployments/sepolia-20260629-r1/$name.json"
  [[ -f $artifact ]]
  cp "$artifact" "$evidence/artifacts/"
done
docker image inspect "$image" --format '{{.Id}}' > "$evidence/postgres-image.txt"
[[ $(cat "$evidence/postgres-image.txt") == "$image" ]]
cargo --version > "$evidence/cargo-version.txt"
rustc --version > "$evidence/rustc-version.txt"
anvil --version > "$evidence/anvil-version.txt"
uname -srmo > "$evidence/host-platform.txt"
getconf _NPROCESSORS_ONLN > "$evidence/host-cpus.txt"
docker version --format '{{.Server.Version}}' > "$evidence/docker-version.txt"
container="r640-$(date -u +%Y%m%dT%H%M%S)-$$"
volume="${container}-data"
owner_token=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
driver_group=''
driver_waited=0
# setsid owns the whole job session, including Cargo's separate process group.
owned_job_alive() {
  [[ -n $driver_group ]] || return 1
  local snapshot alive
  snapshot=$(ps -eo pid=,sid=,stat=) || return 0 # Uncertain absence must fail cleanup.
  alive=$(awk -v sid="$driver_group" -v waited="$driver_waited" '($2 == sid || (!waited && $1 == sid)) && $3 !~ /^Z/ { alive=1 } END { print alive+0 }' <<< "$snapshot") || return 0
  [[ $alive != 0 ]]
}
signal_owned_job() {
  [[ $driver_group =~ ^[1-9][0-9]*$ && $driver_group != $(ps -o sid= -p $$ | tr -d ' ') ]] || return 1
  if ((!driver_waited)) && [[ $(ps -o ppid= -p "$driver_group" | tr -d ' ') == $$ ]]; then
    kill -"$1" "$driver_group" 2>/dev/null
  fi
  pkill -"$1" -s "$driver_group"
}
launch_owned_job() {
  local launch_signal=0
  trap 'launch_signal=130' INT
  trap 'launch_signal=143' TERM
  driver_waited=0
  setsid "$@" &
  driver_group=$!
  trap 'exit 130' INT
  trap 'exit 143' TERM
  ((launch_signal == 0)) || exit "$launch_signal"
}
container_created=0
volume_created=0
cleanup() {
  local outcome=$? cleanup_failed=0
  trap - EXIT INT TERM
  set +e
  if owned_job_alive; then
    signal_owned_job INT
    local until=$((SECONDS + BIGNAME_RESTORE_SHUTDOWN_SECS))
    while owned_job_alive && ((SECONDS < until)); do sleep 1; done
    if owned_job_alive; then
      cleanup_failed=1
      until=$((SECONDS + BIGNAME_RESTORE_SHUTDOWN_SECS))
      while owned_job_alive && ((SECONDS < until)); do signal_owned_job KILL; sleep 1; done
    fi
    owned_job_alive && cleanup_failed=1
  fi
  if ! owned_job_alive; then [[ -z $driver_group ]] || wait "$driver_group" 2>/dev/null; fi
  if ((container_created)); then
    if [[ $(docker inspect --format '{{index .Config.Labels "r640.owner"}}' "$container" 2>/dev/null) == "$owner_token" ]]; then
      if docker logs "$container" > "$evidence/postgres.log" 2>&1; then log_status=0; else log_status=$?; cleanup_failed=1; fi
      printf '%s\n' "$log_status" > "$evidence/postgres-log-exit.txt"
      timeout --kill-after="$BIGNAME_RESTORE_SHUTDOWN_SECS" "$BIGNAME_RESTORE_COMMAND_SECS" docker rm -f "$container" > "$evidence/container-cleanup.txt" 2>&1 || cleanup_failed=1
    else cleanup_failed=1; fi
  fi
  if ((volume_created)); then
    if [[ $(docker volume inspect --format '{{index .Labels "r640.owner"}}' "$volume" 2>/dev/null) == "$owner_token" ]]; then
      timeout --kill-after="$BIGNAME_RESTORE_SHUTDOWN_SECS" "$BIGNAME_RESTORE_COMMAND_SECS" docker volume rm "$volume" > "$evidence/volume-cleanup.txt" 2>&1 || cleanup_failed=1
    else cleanup_failed=1; fi
  fi
  ((cleanup_failed == 0)) || outcome=1
  printf '{"exit_code":%s,"cleanup_failed":%s}\n' "$outcome" "$cleanup_failed" > "$evidence/operation-result.json"
  # Seal closed public evidence only; the manifest and private setup are excluded.
  if ! (cd "$evidence" && find . -type f ! -path './private/*' ! -name 'SHA256SUMS' ! -name 'SHA256SUMS.sha256' -print0 | LC_ALL=C sort -z |
    while IFS= read -r -d '' file; do
      size=$(stat -c '%s' "$file") || exit 1
      digest=$(sha256sum "$file") || exit 1
      printf '%s\t%s\t%s\n' "$file" "$size" "${digest%% *}"
    done) > "$evidence/SHA256SUMS" ||
     ! sha256sum "$evidence/SHA256SUMS" > "$evidence/SHA256SUMS.sha256"; then
    outcome=1
    printf '{"exit_code":1,"seal_failed":true,"cleanup_failed":%s}\n' "$cleanup_failed" > "$evidence/operation-result.json"
  fi
  exit "$outcome"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export R640_ADMIN_PASSWORD=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
export R640_WRITER_PASSWORD=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
export R640_READER_PASSWORD=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
printf 'POSTGRES_PASSWORD=%s\n' "$R640_ADMIN_PASSWORD" > "$private/postgres.env"
volume_created=1
docker volume create --label "r640.owner=$owner_token" "$volume" > "$evidence/volume-create.txt"
container_created=1
docker run --pull=never -d --name "$container" --label "r640.owner=$owner_token" \
  --env-file "$private/postgres.env" -p 127.0.0.1::5432 -v "$volume:/var/lib/postgresql/data" \
  "$image" > "$evidence/container-create.txt"
port=$(docker port "$container" 5432/tcp | sed -n 's/^127\.0\.0\.1://p')
[[ $port =~ ^[0-9]+$ ]]
# First require final TCP service internally; driver also verifies the published host endpoint before setup.
until=$((SECONDS + BIGNAME_RESTORE_READINESS_SECS))
while :; do
  remaining=$((until - SECONDS))
  ((remaining > 0)) || { echo 'Owned PostgreSQL TCP readiness expired' >&2; exit 1; }
  ((remaining <= BIGNAME_RESTORE_COMMAND_SECS)) || remaining=$BIGNAME_RESTORE_COMMAND_SECS
  if running=$(timeout --kill-after="$BIGNAME_RESTORE_SHUTDOWN_SECS" "$remaining" "$docker_binary" inspect --format '{{.State.Running}}' "$container"); then :; else exit $?; fi
  [[ $running == true ]]
  remaining=$((until - SECONDS))
  ((remaining > 0)) || { echo 'Owned PostgreSQL TCP readiness expired' >&2; exit 1; }
  ((remaining <= BIGNAME_RESTORE_COMMAND_SECS)) || remaining=$BIGNAME_RESTORE_COMMAND_SECS
  if timeout --kill-after="$BIGNAME_RESTORE_SHUTDOWN_SECS" "$remaining" "$docker_binary" exec -i "$container" sh -c \
    'read -r PGPASSWORD; export PGPASSWORD; exec psql -X -w -h 127.0.0.1 -p 5432 -U postgres -d postgres -Atc "SELECT 1"' sh \
    < <(printf '%s\n' "$R640_ADMIN_PASSWORD") > "$private/readiness.out" 2> "$private/readiness.err"; then ready_status=0; else ready_status=$?; fi
  ((SECONDS < until)) || { echo 'Owned PostgreSQL TCP readiness expired' >&2; exit 1; }
  ((ready_status != 0)) || break
  if [[ $ready_status == 124 || $ready_status == 137 ]]; then echo "Native readiness command timed out" >&2; exit 1; fi
  # SECONDS and the positive remaining budget are integral; one second cannot exceed it.
  sleep 1
done
for tool in postgres psql pg_dump pg_restore; do docker exec "$container" "$tool" --version; done > "$evidence/postgres-versions.txt"
docker exec "$container" pg_dump --help > "$evidence/pg-dump-help.txt"
rg -- '--restrict-key' "$evidence/pg-dump-help.txt" >/dev/null
export R640_ROOT="$root" R640_EVIDENCE="$evidence" R640_CONTAINER="$container" R640_PORT="$port"
jq -n 'env as $e | {repo_root:$e.R640_ROOT,evidence_dir:$e.R640_EVIDENCE,container_name:$e.R640_CONTAINER,
 admin_url:("postgres://postgres:"+$e.R640_ADMIN_PASSWORD+"@127.0.0.1:"+$e.R640_PORT+"/postgres"),
 admin_password:$e.R640_ADMIN_PASSWORD,original_database:"r640_original",restored_database:"r640_restored",
 writer_password:$e.R640_WRITER_PASSWORD,reader_password:$e.R640_READER_PASSWORD,
 command_timeout_secs:($e.BIGNAME_RESTORE_COMMAND_SECS|tonumber),
 shutdown_timeout_secs:($e.BIGNAME_RESTORE_SHUTDOWN_SECS|tonumber),readiness_timeout_secs:($e.BIGNAME_RESTORE_READINESS_SECS|tonumber),
 progress_timeout_secs:($e.BIGNAME_RESTORE_PROGRESS_SECS|tonumber),poll_secs:($e.BIGNAME_RESTORE_POLL_SECS|tonumber)}' > "$private/config.json"
jq 'del(.admin_url,.admin_password,.writer_password,.reader_password)' "$private/config.json" > "$evidence/configuration.json"
printf '%s\n' "$BIGNAME_RESTORE_RESERVATION" > "$evidence/reservation.txt"
unset R640_ADMIN_PASSWORD R640_WRITER_PASSWORD R640_READER_PASSWORD
export CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR="$private/target"
export BIGNAME_E2E_COMMAND_TIMEOUT_SECS="$BIGNAME_RESTORE_COMMAND_SECS"
export BIGNAME_E2E_READY_TIMEOUT_SECS="$BIGNAME_RESTORE_READINESS_SECS"
printf '%s\n' 'cargo build --locked --manifest-path tests/e2e/Cargo.toml --bin same_release_restore (jobs=1; task-owned target)' > "$evidence/build-command.txt"
launch_owned_job timeout --kill-after="$BIGNAME_RESTORE_SHUTDOWN_SECS" "$BIGNAME_RESTORE_COMMAND_SECS" cargo build --locked --manifest-path tests/e2e/Cargo.toml \
  --bin same_release_restore > "$evidence/driver-build.stdout" 2> "$evidence/driver-build.stderr"
if wait "$driver_group"; then stage_status=0; else stage_status=$?; fi
driver_waited=1
printf '%s\n' "$stage_status" > "$evidence/build-exit.txt"
((stage_status == 0)) || exit "$stage_status"
if owned_job_alive; then echo "Owned session descendants remain after process exit" >&2; exit 1; fi
driver_group=''
printf '%s\n' 'same_release_restore <protected-config-path>' > "$evidence/run-command.txt"
launch_owned_job "$CARGO_TARGET_DIR/debug/same_release_restore" "$private/config.json" \
  > "$evidence/driver.stdout" 2> "$evidence/driver.stderr"
if wait "$driver_group"; then stage_status=0; else stage_status=$?; fi
driver_waited=1
printf '%s\n' "$stage_status" > "$evidence/run-exit.txt"
((stage_status == 0)) || exit "$stage_status"
if owned_job_alive; then echo "Owned session descendants remain after process exit" >&2; exit 1; fi
driver_group=''
