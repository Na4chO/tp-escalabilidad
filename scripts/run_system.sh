#!/usr/bin/env bash
# Runs the system with the parquet files in data/, split among the clients of docker-compose.yaml by
# scripts/split_data.sh, and compares the results of each client with output/expected, computed
# beforehand by scripts/run_expected.sh.
# Usage: scripts/run_system.sh   (the amount of clients comes from `make compose CLIENTS=N`)
set -euo pipefail

cd "$(dirname "$0")/.."

EXPECTED_IMAGE="tp-expected"
CLIENTS=($(docker compose config --services | grep '^client_' | sort -t_ -k2 -n))

if ! ls data/*.parquet >/dev/null 2>&1; then
  echo "No .parquet files in data/, run scripts/download_data.sh first"
  exit 1
fi

cleaned=0
cleanup() {
  [ "$cleaned" -eq 1 ] && return
  cleaned=1
  docker compose logs --no-color > output/logs.txt 2>&1 || true
  docker compose stop -t 5 >/dev/null 2>&1 || true
  docker compose down >/dev/null 2>&1 || true
  echo "Logs saved in output/logs.txt"
}
trap cleanup EXIT

scripts/split_data.sh "${#CLIENTS[@]}"

rm -rf output/client_*
for client in "${CLIENTS[@]}"; do
  mkdir -p "output/$client"
done

echo "Building images"
docker compose build --quiet

echo "Starting system"
start="$(date +%s)"
docker compose up --detach --remove-orphans

running() {
  for client in "${CLIENTS[@]}"; do
    [ "$(docker inspect -f '{{.State.Running}}' "$client")" = "true" ] && return 0
  done
  return 1
}

total_files="$(ls data/*.parquet | wc -l | tr -d ' ')"
echo "Waiting for ${#CLIENTS[@]} clients to finish ($total_files files to send)"
last_progress=""
shown_eofs=0
while running; do
  sleep 10
  sent=0 done_clients=0
  for client in "${CLIENTS[@]}"; do
    logs="$(docker logs --since "$start" "$client" 2>&1 || true)"
    sent=$(( sent + $(grep -c "Reading file" <<< "$logs" || true) ))
    done_clients=$(( done_clients + $(grep -c "Results received" <<< "$logs" || true) ))
  done
  progress="sending file $sent of $total_files, $done_clients of ${#CLIENTS[@]} clients got every result"
  timestamp="[$(( ($(date +%s) - start) / 60 ))m $(( ($(date +%s) - start) % 60 ))s]"
  if [ "$progress" != "$last_progress" ]; then
    echo "  $timestamp $progress"
    last_progress="$progress"
  fi
  # EOFs of each query the server got since the last check, e.g. "EOF query 3 received (2/5) client=1-2"
  eofs="$(docker logs --since "$start" server 2>&1 | grep "EOF query" || true)"
  if [ -n "$eofs" ]; then
    tail -n +$(( shown_eofs + 1 )) <<< "$eofs" | sed -E "s/.*(EOF query .*client=[^ ]*).*/  $timestamp \1/"
    shown_eofs="$(wc -l <<< "$eofs" | tr -d ' ')"
  fi
done
elapsed=$(( $(date +%s) - start ))

for client in "${CLIENTS[@]}"; do
  exit_code="$(docker inspect -f '{{.State.ExitCode}}' "$client")"
  if [ "$exit_code" != "0" ]; then
    echo "$client exited with code $exit_code"
    docker compose logs --tail 20 "$client"
    exit 1
  fi
done
echo "System finished in $((elapsed / 60))m $((elapsed % 60))s (from startup until every client got its results)"

cleanup

if ! docker image inspect "$EXPECTED_IMAGE" >/dev/null 2>&1; then
  echo "No expected results to compare with, run scripts/run_expected.sh"
  exit 0
fi

failed=0
for client in "${CLIENTS[@]}"; do
  expected="output/expected/$client"
  if [ ! -f "$expected/files.txt" ]; then
    echo "SKIP $client: no expected results, run scripts/run_expected.sh"
    failed=1
    continue
  fi
  if ! diff -q <(ls "data/$client") "$expected/files.txt" >/dev/null; then
    echo "SKIP $client: its expected results are from other files, run scripts/run_expected.sh again"
    failed=1
    continue
  fi
  echo "Comparing $client ($(wc -l < "$expected/files.txt" | tr -d ' ') files)"
  docker run --rm -v "$PWD/output:/output:ro" "$EXPECTED_IMAGE" \
    compare "/$expected" "/output/$client" 1 2 3 4 5 || failed=1
done
exit "$failed"
