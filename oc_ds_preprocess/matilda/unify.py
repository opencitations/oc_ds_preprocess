#!/usr/bin/env python3

import csv
import sys
from pathlib import Path


def normalize(value: str) -> str:
    """Normalize identifiers (case-insensitive comparison)."""
    return value.strip().lower()


def merge_csvs(output_file, input_files):
    seen = set()
    rows_out = []

    for file in input_files:
        with open(file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Validate header
            if "citing_id" not in reader.fieldnames or "cited_id" not in reader.fieldnames:
                raise ValueError(
                    f"{file} must contain 'citing_id' and 'cited_id' columns"
                )

            for row in reader:
                citing = normalize(row["citing_id"])
                cited = normalize(row["cited_id"])

                key = (citing, cited)

                # case-insensitive deduplication
                if key not in seen:
                    seen.add(key)
                    rows_out.append({
                        "citing_id": citing,
                        "cited_id": cited,
                    })

    # Write output CSV
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["citing_id", "cited_id"],
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        writer.writerows(rows_out)


def main():
    if len(sys.argv) < 3:
        print(
            "Usage:\n"
            "  python merge_citations.py OUTPUT.csv INPUT1.csv INPUT2.csv ..."
        )
        sys.exit(1)

    output_file = sys.argv[1]
    input_files = sys.argv[2:]

    # Check files exist
    for f in input_files:
        if not Path(f).exists():
            print(f"Error: file not found -> {f}")
            sys.exit(1)

    merge_csvs(output_file, input_files)
    print(f"✅ Merged {len(input_files)} files into {output_file}")


if __name__ == "__main__":
    main()
