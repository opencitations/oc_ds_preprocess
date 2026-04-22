#!/usr/bin/env python3

"""
Main pipeline for processing Matilda data for OpenCitations ingestion.

Steps:
  1. Merge citation tables into a single deduplicated file.
  2. Normalise, validate, and clean the metadata table.
  3. Classify citations by entity location (Matilda / Redis / none).
  4. Validate and clean citation tables where both entities have known metadata.
  5. Remove orphan entities from metadata.

Usage:
  python main.py \
    --citations cits_internal.csv cits_external.csv \
    --metadata metadata.csv \
    --output-dir output/ \
    --redis-host localhost --redis-port 6379 --redis-db 10
"""

import sys
import os

# Ensure sibling modules are importable regardless of working directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
import csv
import shutil
import tempfile
from datetime import date
from pathlib import Path

import redis

from unify import merge_csvs
from prepare_metadata import (
    normalise_table,
    validate as validate_meta,
    prune_metadata,
)
from classify import load_matilda_ids, classify as classify_id
from clean_citations import clean_cit_tables
from clean_metadata import remove_orphan_entities

csv.field_size_limit(sys.maxsize)

# Only these classified tables are validated/cleaned (step 4) and used for
# the metadata closure operation (step 5).  The remaining tables (*_none,
# none_*) are kept as-is.
TABLES_TO_CLEAN = [
    "matilda_matilda",
    "matilda_redis",
    "redis_matilda",
    "redis_redis",
]


def classify_citations(citations_fp, matilda_ids, redis_conn, output_dir):
    """
    Classify citation pairs into 9 categories based on where the citing and
    cited entities are found (Matilda dataset, Redis, or neither).

    Reuses ``classify.load_matilda_ids`` and ``classify.classify``.
    Writes one CSV per category to *output_dir*.
    """
    os.makedirs(output_dir, exist_ok=True)

    outputs = {
        f"{a}_{b}": []
        for a in ("matilda", "redis", "none")
        for b in ("matilda", "redis", "none")
    }

    with open(citations_fp, encoding="utf8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        for row in reader:
            if len(row) < 2:
                continue

            citing_field = row[0].strip()
            cited = row[1].strip()
            citing_ids = citing_field.split()

            cited_loc = classify_id(cited, matilda_ids, redis_conn)

            for citing in citing_ids:
                citing_loc = classify_id(citing, matilda_ids, redis_conn)
                key = f"{citing_loc}_{cited_loc}"
                outputs[key].append((citing, cited))

    for k, rows in outputs.items():
        path = os.path.join(output_dir, f"{k}.csv")
        with open(path, "w", newline="", encoding="utf8") as f:
            w = csv.writer(f)
            w.writerow(["citing_id", "cited_id"])
            w.writerows(rows)
        print(f"  {k}: {len(rows):,} rows")


def main():
    parser = argparse.ArgumentParser(
        description="Matilda data processing pipeline for OpenCitations ingestion.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # --- Input files ---
    parser.add_argument(
        "--citations",
        nargs="+",
        required=True,
        help="Citation CSV files to merge (step 1). Each must have "
        "'citing_id' and 'cited_id' columns.",
    )
    parser.add_argument(
        "--metadata",
        required=True,
        help="Input metadata CSV file (step 2).",
    )

    # --- Output ---
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Root directory for all pipeline outputs.",
    )

    # --- Redis (step 3) ---
    parser.add_argument(
        "--redis-host",
        default="localhost",
        help="Redis host (default: localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port (default: 6379)",
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=10,
        help="Redis DB number (default: 10)",
    )

    # --- Optional ---
    parser.add_argument(
        "--use-lmdb",
        action="store_true",
        help="Use LMDB-backed caching for validation (recommended for large datasets).",
    )
    parser.add_argument(
        "--total-meta-rows",
        type=int,
        default=None,
        help="Total rows in the metadata file (enables progress bar in step 2).",
    )

    args = parser.parse_args()
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    # =================================================================
    # STEP 1 - Merge citation tables
    # =================================================================
    print("\n" + "=" * 60)
    print("STEP 1: Merging citation tables")
    print("=" * 60)

    merged_cits = outdir / "merged_cits.csv"
    merge_csvs(str(merged_cits), args.citations)
    print(f"  -> {merged_cits}")

    # =================================================================
    # STEP 2 - Normalise, validate, and clean metadata
    # =================================================================
    print("\n" + "=" * 60)
    print("STEP 2: Normalising and validating metadata")
    print("=" * 60)

    tmp_meta = Path(tempfile.mkdtemp(dir=outdir))
    normalised_meta = tmp_meta / "normalised_metadata.csv"
    valid_meta = outdir / "valid_meta.csv"

    # 2a. Normalise IDs
    normalise_table(args.metadata, str(normalised_meta), total=args.total_meta_rows)

    # 2b. Validate
    is_valid, report_fp = validate_meta(
        str(normalised_meta), str(tmp_meta), use_lmdb=args.use_lmdb
    )

    # 2c. Prune (or copy if already valid)
    if is_valid:
        print("  No errors found - copying normalised file directly.")
        shutil.copy2(str(normalised_meta), str(valid_meta))
    else:
        prune_metadata(str(normalised_meta), report_fp, str(valid_meta))

    shutil.rmtree(tmp_meta)
    print(f"  -> {valid_meta}")

    # =================================================================
    # STEP 3 - Classify citations by entity location
    # =================================================================
    print("\n" + "=" * 60)
    print("STEP 3: Classifying citations by entity location")
    print("=" * 60)

    classified_dir = outdir / "classified"

    print("  Loading Matilda IDs from valid metadata...")
    matilda_ids = load_matilda_ids(str(valid_meta))

    print(
        f"  Connecting to Redis ({args.redis_host}:{args.redis_port}/{args.redis_db})..."
    )
    r = redis.Redis(
        host=args.redis_host, port=args.redis_port, db=args.redis_db
    )

    classify_citations(str(merged_cits), matilda_ids, r, str(classified_dir))

    # =================================================================
    # STEP 4 - Validate and clean citation tables
    # =================================================================
    print("\n" + "=" * 60)
    print("STEP 4: Validating and cleaning citation tables")
    print("=" * 60)

    # Stage only the 4 relevant tables so clean_cit_tables processes
    # exactly those and nothing else.
    staging_dir = Path(tempfile.mkdtemp(dir=outdir))
    for name in TABLES_TO_CLEAN:
        src = classified_dir / f"{name}.csv"
        if src.exists():
            shutil.copy2(str(src), str(staging_dir / src.name))
        else:
            print(f"  WARNING: {src} not found, skipping.")

    cleaned_cits_dir = outdir / "cleaned_citations"
    clean_cit_tables(
        str(staging_dir), str(cleaned_cits_dir), use_lmdb=args.use_lmdb
    )
    shutil.rmtree(staging_dir)

    # clean_cit_tables writes to a date-stamped subdirectory
    cleaned_subdirs = sorted(Path(cleaned_cits_dir).glob("cleaned_cits_*"))
    if not cleaned_subdirs:
        print("  ERROR: No cleaned citation output found.")
        sys.exit(1)
    actual_cleaned_dir = cleaned_subdirs[-1]

    # =================================================================
    # STEP 5 - Remove orphan entities from metadata
    # =================================================================
    print("\n" + "=" * 60)
    print("STEP 5: Removing orphan entities from metadata")
    print("=" * 60)

    final_meta = outdir / "final_meta.csv"
    cits_fps = [str(f) for f in sorted(actual_cleaned_dir.glob("*.csv"))]

    remove_orphan_entities(
        meta_fp=str(valid_meta),
        cits_fps=cits_fps,
        final_meta_fp=str(final_meta),
    )

    # =================================================================
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print(f"  Final metadata    : {final_meta}")
    print(f"  Cleaned citations : {actual_cleaned_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
