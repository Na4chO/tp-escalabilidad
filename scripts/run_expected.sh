#!/usr/bin/env bash
# Computes the expected results of each client of docker-compose.yaml, one at a time, over the files that
# scripts/split_data.sh gives it. They go to output/expected/<client>, where scripts/run_system.sh compares.
# Usage: scripts/run_expected.sh   (run it again whenever data/ or the amount of clients changes)
set -euo pipefail

cd "$(dirname "$0")/.."

EXPECTED_IMAGE="tp-expected"
CLIENTS=($(docker compose config --services | grep '^client_' | sort -t_ -k2 -n))

if ! ls data/*.parquet >/dev/null 2>&1; then
  echo "No .parquet files in data/, run scripts/download_data.sh first"
  exit 1
fi

scripts/split_data.sh "${#CLIENTS[@]}"
docker build --quiet --tag "$EXPECTED_IMAGE" scripts/expected >/dev/null
rm -rf output/expected
mkdir -p output/expected

failed=0
for client in "${CLIENTS[@]}"; do
  echo "Computing expected results of $client ($(ls "data/$client" | wc -l | tr -d ' ') files)"
  start="$(date +%s)"
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
  # The files used, so scripts/run_system.sh can tell if the expected results are still valid
  ls "data/$client" > "output/expected/$client/files.txt"
  sed 's/^/  /' "output/expected/$client.log"
  echo "  done in $(( $(date +%s) - start ))s"
done
exit "$failed"
