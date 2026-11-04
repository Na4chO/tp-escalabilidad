"""Reference implementation of the 5 queries, used to check the system results.

  expected.py generate <data_dir> <output_dir>
  expected.py compare <expected_dir> <actual_dir> <query> [<query> ...]

Both directories hold query<N>.csv files with the same format the client writes.
"""

import base64
import collections
import csv
import glob
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

import pyarrow.parquet as pq
from nltk.corpus import stopwords

TOP_SIZE = 20
csv.field_size_limit(sys.maxsize)  # query 2 rows carry whole images
LAST_OLD_YEAR = 2020
STOPWORDS = set(stopwords.words("english")) | set(stopwords.words("french"))
WORD = re.compile(r"\w+")
NUMBER = re.compile(r"^[+-]?(([0-9]+\.?[0-9]*)|(\.[0-9]+))$")
# Wikimedia rate limits user agents without contact info
USER_AGENT = "SistemasDistribuidos-Grupo29/1.0 (https://github.com/Na4chO/tp-escalabilidad)"
COLUMNS = ["url", "name", "abstract", "date_modified", "in_language", "image", "sections"]


def words(abstract):
    """Unique lowercase words of an abstract, without stopwords nor numbers."""
    # Python lowercases "İ" as "i" + combining dot, Go as a plain "i"
    text = (abstract or "").lower().replace("i̇", "i")
    unique = dict.fromkeys(w for w in WORD.findall(text) if w not in STOPWORDS and not NUMBER.match(w))
    return list(unique)


def has_biography(sections):
    try:
        parsed = json.loads(sections or "[]")
    except json.JSONDecodeError:
        return False
    return any((s.get("name") or "").strip().lower() in ("biography", "biographie") for s in parsed)


def image_exists(url, attempts=4):
    """The dataset has image urls that no longer exist (e.g. renamed files), the system skips those articles."""
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=30):
                return True
        except urllib.error.HTTPError as error:
            if error.code != 429 or attempt == attempts - 1:
                return False
        except urllib.error.URLError:
            if attempt == attempts - 1:
                return False
        time.sleep(2**attempt)
    return False


def generate(data_dir, output_dir):
    files = sorted(glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True))

    q1, q2, new = [], [], []
    vocabulary = {"en": set(), "fr": set()}
    frequency = collections.Counter()
    old_words, old_articles = 0, 0

    for path in files:
        for row in pq.read_table(path, columns=COLUMNS).to_pylist():
            date = row["date_modified"]
            language = (row["in_language"] or {}).get("identifier")
            image = (row["image"] or {}).get("content_url")
            abstract_words = words(row["abstract"])

            frequency.update(abstract_words)
            if language in vocabulary:
                vocabulary[language].update(abstract_words)

            if date.year <= LAST_OLD_YEAR:
                old_words += len(abstract_words)
                old_articles += 1
                if language == "en":
                    q1.append([row["url"], date.strftime("%Y-%m-%dT%H:%M:%SZ")])
                if has_biography(row["sections"]) and image:
                    q2.append([row["name"], row["url"], image])
            else:
                new.append([row["url"], len(abstract_words)])

    missing_images = [row for row in q2 if not image_exists(row[2])]
    for name, url, image in missing_images:
        print(f"  query2: skipping {url}, image not found: {image}")
    q2 = [[name, url] for name, url, image in q2 if [name, url, image] not in missing_images]

    average = old_words / old_articles if old_articles else 0
    top = sorted(frequency.items(), key=lambda item: (-item[1], item[0]))[:TOP_SIZE]
    results = {
        1: q1,
        2: q2,
        3: [[w] for w in sorted(vocabulary["en"] & vocabulary["fr"])],
        4: [[w, c] for w, c in top],
        5: [[url, count] for url, count in new if count > average],
    }

    os.makedirs(output_dir, exist_ok=True)
    for query, rows in results.items():
        with open(os.path.join(output_dir, f"query{query}.csv"), "w", newline="") as file:
            csv.writer(file).writerows(rows)
    print(f"Expected results from {len(files)} files (old average {average:.4f}):")
    for query, rows in results.items():
        print(f"  query{query}.csv: {len(rows)} rows")


def read(directory, query):
    path = os.path.join(directory, f"query{query}.csv")
    if not os.path.exists(path):
        return []
    with open(path, newline="") as file:
        return [tuple(row) for row in csv.reader(file)]


def compare_rows(expected, actual):
    """Rows can arrive in any order, so they are compared as multisets."""
    expected, actual = collections.Counter(expected), collections.Counter(actual)
    missing, extra = list((expected - actual).elements()), list((actual - expected).elements())
    return missing, extra


def compare_top(expected, actual):
    """Words tied at the last position can be any of them, so only their count is checked."""
    expected_counts = sorted((int(c) for _, c in expected), reverse=True)
    actual_counts = sorted((int(c) for _, c in actual), reverse=True)
    if expected_counts != actual_counts:
        return compare_rows(expected, actual)
    boundary = expected_counts[-1] if expected_counts else 0
    expected_sure = [row for row in expected if int(row[1]) > boundary]
    actual_sure = [row for row in actual if int(row[1]) > boundary]
    return compare_rows(expected_sure, actual_sure)


def compare(expected_dir, actual_dir, queries):
    failed = False
    for query in queries:
        expected, actual = read(expected_dir, query), read(actual_dir, query)
        if query == 2:
            # Image bytes come from Wikimedia, so only check that there is one
            without_image = [row for row in actual if len(row) < 3 or not base64.b64decode(row[2])]
            if without_image:
                print(f"FAIL query{query}: {len(without_image)} rows without image, e.g. {without_image[0][:2]}")
                failed = True
            actual = [row[:2] for row in actual]
        missing, extra = compare_top(expected, actual) if query == 4 else compare_rows(expected, actual)

        if missing or extra:
            failed = True
            print(f"FAIL query{query}: expected {len(expected)} rows, got {len(actual)}")
            for row in missing[:5]:
                print(f"       missing {row}")
            for row in extra[:5]:
                print(f"       extra   {row}")
        else:
            print(f"OK   query{query}: {len(actual)} rows")
    return 1 if failed else 0


if __name__ == "__main__":
    command, args = sys.argv[1], sys.argv[2:]
    if command == "generate":
        generate(args[0], args[1])
    elif command == "compare":
        sys.exit(compare(args[0], args[1], [int(q) for q in args[2:]]))
    else:
        sys.exit(f"Unknown command {command}")
