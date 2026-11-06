#!/usr/bin/env bash
# Launches N clients in parallel against a running system. Every client processes data/ and writes to
# its own output/client_run_<i>.
# Usage: scripts/run_clients.sh <clients>
#   WAIT=1  wait for every client and return non-zero if any failed
#   KEEP=1  keep the client containers after they finish
set -euo pipefail

cd "$(dirname "$0")/.."

N="${1:?usage: scripts/run_clients.sh <clients>}"
WAIT="${WAIT:-0}"
KEEP="${KEEP:-0}"
INPUT_DIR="${INPUT_DIR:-data}"

if [ "$(docker inspect -f '{{.State.Running}}' server 2>/dev/null)" != "true" ]; then
  echo "server is not running; start it with: make system"
  exit 1
fi

if ! ls "$INPUT_DIR"/*.parquet >/dev/null 2>&1; then
  echo "no .parquet files in $INPUT_DIR"
  exit 1
fi

docker compose -f docker-compose.yaml -f docker-compose.clients.yaml build client_run

for (( i = 0; i < N; i++ )); do
  docker rm -f "client_run_$i" >/dev/null 2>&1 || true
  mkdir -p "output/client_run_$i"

  flags=(-d -T --no-deps)
  if [ "$WAIT" != "1" ] && [ "$KEEP" != "1" ]; then
    flags+=(--rm)
  fi

  docker compose -f docker-compose.yaml -f docker-compose.clients.yaml run "${flags[@]}" \
    --name "client_run_$i" \
    -v "$PWD/$INPUT_DIR:/data:ro" \
    -v "$PWD/output/client_run_$i:/output" \
    client_run
done

if [ "$WAIT" != "1" ]; then
  exit 0
fi

status=0
for (( i = 0; i < N; i++ )); do
  code="$(docker wait "client_run_$i")" || code=1
  echo "client_run_$i exited with code $code"
  [ "$code" = "0" ] || status=1
  if [ "$KEEP" != "1" ]; then
    docker rm -f "client_run_$i" >/dev/null 2>&1 || true
  fi
done

exit "$status"
