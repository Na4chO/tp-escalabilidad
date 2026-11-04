#!/usr/bin/env bash
# Runs the system with the parquet files in data/ and compares the client results with the
# ones computed by scripts/expected/expected.py over the same files.
# Usage: scripts/run_test.sh
# Env:
#   TIMEOUT  seconds to wait for the client (default 600)
#   QUERIES  queries to compare (default "1 2 3 4 5")
set -euo pipefail

cd "$(dirname "$0")/.."

QUERIES="${QUERIES:-1 2 3 4 5}"
EXPECTED_DIR="output/expected"
EXPECTED_IMAGE="tp-expected"
TIMEOUT="${TIMEOUT:-600}"
OUTPUT_DIR="output/client_0"
CLIENT="client_0"
SERVICES=(rabbitmq server metadata_filter image_fetcher abstract_preprocessing frequency_collector
  top_aggregator average_calculator abstract_word_counter "$CLIENT")

if ! ls data/*.parquet >/dev/null 2>&1; then
  echo "No .parquet files in data/, run scripts/download_data.sh first"
  exit 1
fi

cleanup() {
  docker compose logs --no-color > output/logs.txt 2>&1 || true
  docker compose stop -t 5 >/dev/null 2>&1 || true
  docker compose down >/dev/null 2>&1 || true
  echo "Logs saved in output/logs.txt"
}
trap cleanup EXIT

rm -rf "$OUTPUT_DIR" "$EXPECTED_DIR"
mkdir -p "$OUTPUT_DIR" "$EXPECTED_DIR"

echo "Computing expected results"
docker build --quiet --tag "$EXPECTED_IMAGE" scripts/expected >/dev/null
docker run --rm -v "$PWD/data:/data:ro" -v "$PWD/output:/output" "$EXPECTED_IMAGE" \
  generate /data "/$EXPECTED_DIR"

echo "Starting system"
docker compose up --build --detach "${SERVICES[@]}"

echo "Waiting for $CLIENT to finish"
elapsed=0
while [ "$(docker inspect -f '{{.State.Running}}' "$CLIENT")" = "true" ]; do
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    echo "Timeout after ${TIMEOUT}s"
    exit 1
  fi
  sleep 2
  elapsed=$((elapsed + 2))
done

exit_code="$(docker inspect -f '{{.State.ExitCode}}' "$CLIENT")"
if [ "$exit_code" != "0" ]; then
  echo "$CLIENT exited with code $exit_code"
  docker compose logs --tail 20 "$CLIENT"
  exit 1
fi
echo "$CLIENT finished in ~${elapsed}s"

for file in "$OUTPUT_DIR"/query*.csv; do
  echo "  $(basename "$file"): $(wc -l < "$file" | tr -d ' ') rows"
done

echo "Comparing results"
# shellcheck disable=SC2086
docker run --rm -v "$PWD/output:/output:ro" "$EXPECTED_IMAGE" \
  compare "/$EXPECTED_DIR" "/$OUTPUT_DIR" $QUERIES
