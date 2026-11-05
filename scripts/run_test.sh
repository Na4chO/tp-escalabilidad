#!/usr/bin/env bash
# Runs the system with the parquet files in data/ and compares the client results with the
# ones computed by scripts/expected/expected.py over the same files.
# Usage: scripts/run_test.sh
set -euo pipefail

cd "$(dirname "$0")/.."

EXPECTED_DIR="output/expected"
EXPECTED_IMAGE="tp-expected"
OUTPUT_DIR="output/client_0"
CLIENT="client_0"

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

echo "Starting system"
docker compose up --build --detach --remove-orphans

echo "Waiting for $CLIENT to finish"
elapsed=0
while [ "$(docker inspect -f '{{.State.Running}}' "$CLIENT")" = "true" ]; do
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

echo "Computing expected results"
docker build --quiet --tag "$EXPECTED_IMAGE" scripts/expected >/dev/null
docker run --rm -v "$PWD/data:/data:ro" -v "$PWD/output:/output" "$EXPECTED_IMAGE" \
  generate /data "/$EXPECTED_DIR"

echo "Comparing results"
docker run --rm -v "$PWD/output:/output:ro" "$EXPECTED_IMAGE" \
  compare "/$EXPECTED_DIR" "/$OUTPUT_DIR" 1 2 3 4 5
