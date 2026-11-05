#!/usr/bin/env bash
# Downloads a fraction of the Kaggle dataset into data/, keeping the share of each language.
# Usage: scripts/download_data.sh [fraction]   (default 0.1, e.g. 0.5 downloads half of the dataset)
# Files already in data/ are skipped, so a bigger fraction only downloads what is missing.
set -euo pipefail

DATASET="wikimedia-foundation/wikipedia-structured-contents"
API="https://www.kaggle.com/api/v1/datasets"
DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"
FRACTION="${1:-0.1}"
PARALLEL_DOWNLOADS=4

# Lists every parquet of the dataset (the API pages by 200) and keeps the first files of each language
select_files() {
  python3 -I - "$API/list/$DATASET" "$FRACTION" <<'EOF'
import json, math, sys, urllib.request
url, fraction = sys.argv[1], float(sys.argv[2])
files, token = [], ""
while True:
    page = url + "?pageSize=200" + (f"&pageToken={token}" if token else "")
    data = json.load(urllib.request.urlopen(page, timeout=60))
    files += [f["name"] for f in data["datasetFiles"] if f["name"].endswith(".parquet")]
    token = data.get("nextPageTokenNullable")
    if not token:
        break
by_language = {}
for name in sorted(set(files)):
    by_language.setdefault(name.split("/")[0], []).append(name)
for names in by_language.values():
    print("\n".join(names[:math.ceil(len(names) * fraction)]))
EOF
}

download() {
  local file="$1" dest="$DATA_DIR/$(basename "$1")"
  if [ -f "$dest" ]; then
    return
  fi
  curl -fsSL --retry 3 -o "$dest.tmp" "$API/download/$DATASET?fileName=$file"
  mv "$dest.tmp" "$dest"
  echo "Downloaded $(basename "$file")"
}
export -f download
export API DATASET DATA_DIR

mkdir -p "$DATA_DIR"
files="$(select_files)"
echo "Downloading $(echo "$files" | wc -l | tr -d ' ') files ($FRACTION of the dataset) into data/"
echo "$files" | xargs -P "$PARALLEL_DOWNLOADS" -I {} bash -c 'download "$1"' _ {}

# The client sends every file in data/, so files outside the selection change the run
extra="$(comm -13 <(echo "$files" | xargs -n1 basename | sort) <(ls "$DATA_DIR" | grep '\.parquet$' | sort) || true)"
if [ -n "$extra" ]; then
  echo "Warning: these files in data/ are not part of the selection and will also be sent:"
  echo "$extra" | sed 's/^/  /'
fi
