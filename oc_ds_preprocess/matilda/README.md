# Matilda Data Processing Pipeline

Pipeline for preparing citations and metadata from the [Matilda](https://matilda.unibo.it/) project for ingestion into [OpenCitations Index](https://index.opencitations.net/) (citations) and [OpenCitations Meta](https://meta.opencitations.net/) (metadata).

## Overview

The Matilda project provides two citation tables and one metadata table. The two citation tables are separated by the **availability** of the citing and cited entities within Matilda:

- **Internal citations** — both the citing and cited entities are registered in Matilda.
- **External citations** — the citing entity is registered in Matilda, but the cited entity is not.

The pipeline merges, normalises, validates, and classifies these inputs to produce clean citation tables and a metadata table ready for OpenCitations ingestion.

## Prerequisites

### Python packages

| Package | Used by |
|---|---|
| `redis` | `classify.py`, `main.py` |
| `tqdm` | `prepare_metadata.py`, `clean_metadata.py` |
| `oc_validator` | `prepare_metadata.py`, `clean_citations.py` |
| `oc_pruner` | `prepare_metadata.py`, `clean_citations.py` |

### External services

- **Redis** — a running Redis instance holding the entity IDs already stored in OpenCitations Meta. Required for step 3 (classification).

## Input data

The pipeline expects three input files:

| File | Format | Required columns |
|---|---|---|
| Internal citations | CSV | `citing_id`, `cited_id` |
| External citations | CSV | `citing_id`, `cited_id` |
| Metadata | CSV | `id`, plus other metadata columns matching the OpenCitations Meta schema |

All citation IDs are normalised to lowercase with stripped whitespace before processing.

## Pipeline steps

### Step 1 — Merge citation tables

**Script:** `unify.py` &nbsp;|&nbsp; **Function:** `merge_csvs(output_file, input_files)`

Merges the two (or more) citation CSVs into a single deduplicated table. Deduplication is case-insensitive: two rows are considered duplicates if both `citing_id` and `cited_id` match after lowercasing and stripping whitespace.

**Output:** `merged_cits.csv`

### Step 2 — Normalise, validate, and clean metadata

**Script:** `prepare_metadata.py` &nbsp;|&nbsp; **Functions:** `normalise_table`, `validate`, `prune_metadata`

Three sub-steps:

1. **Normalisation** — all values in the `id` column are lowercased and stripped of surrounding whitespace.
2. **Validation** — the normalised table is validated using `oc_validator`. ID existence is not verified (no calls to external APIs or the OC SPARQL endpoint).
3. **Pruning** — rows containing validation **errors** are removed. Rows that only have **warnings** are kept.

**Output:** `valid_meta.csv`

### Step 3 — Classify citations by entity location

**Script:** `classify.py` &nbsp;|&nbsp; **Functions:** `load_matilda_ids`, `classify`, `redis_exists`

Each citation pair is classified by looking up both the citing and cited ID in two locations:

- **Matilda** — the set of valid IDs from `valid_meta.csv` (output of step 2).
- **Redis** — the OpenCitations Meta Redis database.

This produces 9 classification categories (`{citing_loc}_{cited_loc}`), where each location is one of `matilda`, `redis`, or `none`:

| Category | Citing entity | Cited entity |
|---|---|---|
| `matilda_matilda` | Matilda | Matilda |
| `matilda_redis` | Matilda | Redis |
| `matilda_none` | Matilda | neither |
| `redis_matilda` | Redis | Matilda |
| `redis_redis` | Redis | Redis |
| `redis_none` | Redis | neither |
| `none_matilda` | neither | Matilda |
| `none_redis` | neither | Redis |
| `none_none` | neither | neither |

Each category is written to a separate CSV file in the `classified/` directory.

**Output:** `classified/<category>.csv` (9 files)

### Step 4 — Validate and clean citation tables

**Script:** `clean_citations.py` &nbsp;|&nbsp; **Function:** `clean_cit_tables(input_dir, output_dir, use_lmdb)`

Only the four tables where **both** the citing and cited entity have known metadata are validated and cleaned:

- `matilda_matilda.csv`
- `matilda_redis.csv`
- `redis_matilda.csv`
- `redis_redis.csv`

Validation is performed using `oc_validator`; rows with errors are pruned using `oc_pruner`. Rows with warnings only are kept. The remaining five tables (`*_none`, `none_*`) are left as-is.

**Output:** `cleaned_citations/cleaned_cits_<date>/` (4 cleaned CSV files)

### Step 5 — Remove orphan entities from metadata

**Script:** `clean_metadata.py` &nbsp;|&nbsp; **Function:** `remove_orphan_entities(meta_fp, cits_fps, final_meta_fp)`

The metadata table from step 2 may contain entities that are not involved in any of the valid citations from step 4. This step performs a **closure operation**: it scans all cleaned citation tables, collects every citing and cited ID, and removes from the metadata any entity whose ID does not appear in at least one valid citation.

Sidecar files are written to the working directory for auditing purposes:

- `ids_in_matilda_meta.jsonl` — all IDs found in the input metadata.
- `ids_without_valid_cits.jsonl` — IDs that were removed (orphans).

**Output:** `final_meta.csv`

## Output structure

After a successful run, the output directory contains:

```
<output-dir>/
  merged_cits.csv                            # step 1
  valid_meta.csv                             # step 2
  classified/                                # step 3
    matilda_matilda.csv
    matilda_redis.csv
    matilda_none.csv
    redis_matilda.csv
    redis_redis.csv
    redis_none.csv
    none_matilda.csv
    none_redis.csv
    none_none.csv
  cleaned_citations/                         # step 4
    cleaned_cits_<YYYY-MM-DD>/
      matilda_matilda.csv
      matilda_redis.csv
      redis_matilda.csv
      redis_redis.csv
  final_meta.csv                             # step 5
```

## Usage

### Full pipeline (`main.py`)

```bash
python main.py \
  --citations cits_internal.csv cits_external.csv \
  --metadata metadata.csv \
  --output-dir output/ \
  --redis-host localhost --redis-port 6379 --redis-db 10
```

**Arguments:**

| Argument | Required | Default | Description |
|---|---|---|---|
| `--citations` | yes | — | Paths to the citation CSV files to merge (2 or more) |
| `--metadata` | yes | — | Path to the input metadata CSV file |
| `--output-dir` | yes | — | Root directory for all pipeline outputs |
| `--redis-host` | no | `localhost` | Redis host address |
| `--redis-port` | no | `6379` | Redis port |
| `--redis-db` | no | `10` | Redis database number |
| `--use-lmdb` | no | off | Enable LMDB-backed caching for validation |
| `--total-meta-rows` | no | — | Total rows in the metadata file (enables progress bar in step 2) |

### Individual scripts

Each step can also be run independently via its own script:

**Step 1 — merge citations:**
```bash
python unify.py merged.csv input1.csv input2.csv
```

**Step 2 — prepare metadata:**
```bash
python prepare_metadata.py -i metadata.csv -o valid_meta.csv [-t <num_rows>]
```

**Step 3 — classify citations:**
```bash
python classify.py --citations merged_cits.csv --matilda valid_meta.csv \
  [--redis-host localhost] [--redis-port 6379] [--redis-db 10] \
  [--output-dir collections]
```

**Step 4 — clean citation tables:**
```bash
python clean_citations.py -i classified/ -o cleaned_citations/ [--use_lmdb]
```

**Step 5 — remove orphan metadata:**
```bash
python clean_metadata.py --meta valid_meta.csv \
  --cits cleaned_citations/cleaned_cits_2025-01-15/matilda_matilda.csv \
         cleaned_citations/cleaned_cits_2025-01-15/matilda_redis.csv \
         cleaned_citations/cleaned_cits_2025-01-15/redis_matilda.csv \
         cleaned_citations/cleaned_cits_2025-01-15/redis_redis.csv \
  --out final_meta.csv
```

## Module reference

### `unify.py`

Merges multiple citation CSVs into one, performing case-insensitive deduplication.

| Function | Description |
|---|---|
| `normalize(value)` | Strips whitespace and lowercases a string. |
| `merge_csvs(output_file, input_files)` | Reads all input CSVs, deduplicates, and writes the merged result. |

### `prepare_metadata.py`

Normalises, validates, and prunes the metadata table.

| Function | Description |
|---|---|
| `normalise_value(x)` | Strips whitespace and lowercases a string. |
| `get_fieldnames(fp)` | Returns the column headers of a CSV file. |
| `normalise_table(in_path, out_path, total)` | Normalises the `id` column of the input CSV and writes to output. |
| `validate(norm_fp, out_dir, use_lmdb)` | Runs `oc_validator` on the normalised metadata; returns `(is_valid, report_path)`. |
| `prune_metadata(norm_fp, report_path, output_path)` | Removes error rows using `oc_pruner`; warnings are kept. |

### `classify.py`

Classifies citation pairs based on entity membership in Matilda or Redis.

| Function | Description |
|---|---|
| `normalize(x)` | Strips whitespace and lowercases a string. |
| `load_matilda_ids(path)` | Loads all IDs (both original and normalised form) from a metadata CSV into a set. |
| `redis_exists(r, key)` | Checks whether a key exists in Redis (tries both original and normalised form). |
| `classify(id_val, matilda_ids, r)` | Returns `"matilda"`, `"redis"`, or `"none"` for a given ID. |

### `clean_citations.py`

Validates and prunes citation tables in a directory.

| Function | Description |
|---|---|
| `clean_cit_tables(input_dir, output_dir, use_lmdb)` | Validates every CSV in `input_dir` with `oc_validator` and prunes error rows with `oc_pruner`. Valid tables are moved as-is. Output is written to a date-stamped subdirectory of `output_dir`. |

### `clean_metadata.py`

Performs closure on the metadata table by removing entities not involved in any valid citation.

| Function | Description |
|---|---|
| `get_all_ids(meta_fp)` | Maps every ID in the metadata to its row index. Also writes `ids_in_matilda_meta.jsonl`. |
| `get_missing_cits_ids(meta_ids, cits_fps)` | Returns IDs from `meta_ids` that do not appear in any of the citation tables. Also writes `ids_without_valid_cits.jsonl`. |
| `remove_orphan_entities(meta_fp, cits_fps, final_meta_fp)` | Writes a new metadata CSV containing only rows whose IDs appear in at least one citation table. |
