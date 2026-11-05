#!/usr/bin/env bash
# Runs the system with the parquet files in data/, split among the clients of docker-compose.yaml by
# scripts/split_data.sh, and compares the results of each client with the ones computed by
# scripts/expected (a Go program) over its files, one client at a time.
# Usage: scripts/run_test.sh   (the amount of clients comes from `make compose CLIENTS=N`)
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

rm -rf output/client_* output/expected
mkdir -p output/expected
for client in "${CLIENTS[@]}"; do
  mkdir -p "output/$client"
done

echo "Building images"
docker compose build --quiet
docker build --quiet --tag "$EXPECTED_IMAGE" scripts/expected >/dev/null

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

# The system is no longer needed, stopping it frees its memory for the expected results
cleanup

failed=0
for client in "${CLIENTS[@]}"; do
  echo "Computing expected results of $client"
  status=0
  docker run --rm -v "$PWD/data:/data" -v "$PWD/output:/output" -e IMAGE_CACHE=/data/image_status.json \
    "$EXPECTED_IMAGE" generate "/data/$client" "/output/expected/$client" > "output/expected/$client.log" 2>&1 || status=$?
  if [ "$status" -ne 0 ]; then
    reason=""
    [ "$status" -eq 137 ] && reason=", killed, probably out of memory"
    echo "FAIL expected results of $client (exit $status$reason), see output/expected/$client.log"
    failed=1
    continue
  fi
  echo "Comparing $client ($(ls "data/$client" | wc -l | tr -d ' ') files)"
  docker run --rm -v "$PWD/output:/output:ro" "$EXPECTED_IMAGE" \
    compare "/output/expected/$client" "/output/$client" 1 2 3 4 5 || failed=1
done
exit "$failed"
