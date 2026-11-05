#!/usr/bin/env bash
# Splits the parquet files of data/ among the clients: data/client_<i>/ gets one of every N files.
# The files are sorted by language, so every client gets a mix of both. They are hard links, so nothing is copied.
# Usage: scripts/split_data.sh <clients>
set -euo pipefail

cd "$(dirname "$0")/.."

CLIENTS="${1:?usage: scripts/split_data.sh <clients>}"

rm -rf data/client_*
for (( i = 0; i < CLIENTS; i++ )); do
  mkdir -p "data/client_$i"
done

i=0
for file in data/*.parquet; do
  ln "$file" "data/client_$(( i % CLIENTS ))/$(basename "$file")"
  i=$(( i + 1 ))
done

for (( i = 0; i < CLIENTS; i++ )); do
  echo "data/client_$i: $(ls "data/client_$i" | wc -l | tr -d ' ') files"
done
