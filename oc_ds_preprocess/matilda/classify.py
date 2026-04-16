#!/usr/bin/env python3
"""
Classifies citation pairs from a CSV file into categories based on whether
citing/cited IDs belong to Matilda dataset or Redis cache.

Categories: matilda_matilda, matilda_redis, etc.
Writes separate CSV files for each category in --output-dir.
"""

import csv
import argparse
import redis
import os
import sys

# Allow reading very large CSV fields
csv.field_size_limit(sys.maxsize)


def normalize(x: str) -> str:
    """Normalize ID string: strip whitespace and convert to lowercase."""
    return x.strip().lower()


def load_matilda_ids(path: str) -> set[str]:
    """
    Load Matilda dataset IDs into a set, including both normalized and original forms.

    Args:
        path: Path to Matilda CSV with 'id' column.

    Returns:
        Set of IDs for fast lookup.
    """
    ids = set()

    with open(path, encoding="utf8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            i = row["id"].strip()
            ids.add(i)
            ids.add(normalize(i))

    return ids


def redis_exists(r: redis.Redis, key: str) -> bool:
    """
    Check if key exists in Redis (exact or normalized form).

    Args:
        r: Redis connection.
        key: ID to check.

    Returns:
        True if key exists in either form.
    """
    if r.exists(key):
        return True

    k = normalize(key)
    if k != key and r.exists(k):
        return True

    return False


def classify(id_val: str, matilda_ids: set[str], r: redis.Redis) -> str:
    """
    Classify single ID into one of: 'matilda', 'redis', 'none'.

    Args:
        id_val: Citation ID to classify.
        matilda_ids: Set of Matilda IDs.
        r: Redis connection.

    Returns:
        Classification string.
    """
    if id_val in matilda_ids or normalize(id_val) in matilda_ids:
        return "matilda"

    if redis_exists(r, id_val):
        return "redis"

    return "none"


def main():
    parser = argparse.ArgumentParser(
        description="Classify citation pairs by Matilda/Redis membership."
    )
    parser.add_argument("--citations", required=True, help="Input CSV: citing_id,cited_id")
    parser.add_argument("--matilda", required=True, help="Matilda CSV with 'id' column")
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-db", type=int, default=10)
    parser.add_argument("--output-dir", default="collections")

    args = parser.parse_args()

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    print("Loading Matilda IDs...")
    matilda_ids = load_matilda_ids(args.matilda)

    print("Connecting to Redis...")
    r = redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db
    )

    # Dict to hold rows for each category (e.g., "matilda_redis")
    outputs = {
        "matilda_matilda": [],
        "matilda_redis": [],
        "matilda_none": [],
        "redis_matilda": [],
        "redis_redis": [],
        "redis_none": [],
        "none_matilda": [],
        "none_redis": [],
        "none_none": []
    }

    with open(args.citations, encoding="utf8") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row

        for row in reader:
            if len(row) < 2:
                continue

            citing_field = row[0].strip()
            cited = row[1].strip()

            citing_ids = citing_field.split()  # Handle multiple citing IDs per row

            cited_loc = classify(cited, matilda_ids, r)

            for citing in citing_ids:
                citing_loc = classify(citing, matilda_ids, r)

                key = f"{citing_loc}_{cited_loc}"
                if key not in outputs:
                    outputs[key] = []

                outputs[key].append((citing, cited))

    print("Writing categorized CSVs...")

    for k, rows in outputs.items():
        path = os.path.join(args.output_dir, f"{k}.csv")

        with open(path, "w", newline="", encoding="utf8") as f:
            w = csv.writer(f)
            w.writerow(["citing_id", "cited_id"])  # Add header

            for r_ in rows:
                w.writerow(r_)

        print(f"{k}: {len(rows)} rows")


if __name__ == "__main__":
    main()
