#!/usr/bin/env bash
# Downloads a few parquet files of the Kaggle dataset into data/.
# Usage: scripts/download_data.sh [file ...]   (paths inside the dataset, e.g. frwiki/data/frwiki_namespace_0_00000.parquet)
set -euo pipefail

DATASET="wikimedia-foundation/wikipedia-structured-contents"
DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"

FILES=("$@")
if [ ${#FILES[@]} -eq 0 ]; then
  FILES=(
    "enwiki/data/enwiki_namespace_0_00264.parquet"
    "frwiki/data/frwiki_namespace_0_00000.parquet"
  )
fi

mkdir -p "$DATA_DIR"
for file in "${FILES[@]}"; do
  dest="$DATA_DIR/$(basename "$file")"
  if [ -f "$dest" ]; then
    echo "Already downloaded: $dest"
    continue
  fi
  echo "Downloading $file"
  curl -fL --progress-bar -o "$dest.tmp" \
    "https://www.kaggle.com/api/v1/datasets/download/$DATASET?fileName=$file"
  mv "$dest.tmp" "$dest"
done
