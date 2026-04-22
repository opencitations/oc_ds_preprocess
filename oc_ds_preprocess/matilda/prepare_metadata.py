#!/usr/bin/env python3

import csv
import argparse
import os
import sys
from tqdm import tqdm
import tempfile
import logging
from oc_validator.main import Validator
from oc_pruner import prune
from oc_pruner.config import PrunerConfig
import shutil

"""
Prepares Matilda metadata table for further processing: normalises the
values in the 'id' field, validates the normalised document, then removes 
invalid rows containing ERRORS (warnings are ignored, therefore rows
that only contain warnings are kept in the final document).
"""

def normalise_value(x):
    return x.strip().lower()

def get_fieldnames(fp):
    with open(fp, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = [str(x) for x in next(reader).keys()]
    return fieldnames


def normalise_table(in_path, out_path, total=None):
    """
    Normalises the values in the 'id' field of the input CSV file 
    (strips leading/trailing whitespace and converts to lowercase)
    and writes the normalised table to the output path.
    """

    csv.field_size_limit(sys.maxsize)
    changed_id_vals=0

    with open(in_path, encoding="utf8") as f_in, open(out_path, 'w', encoding='utf-8') as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames, dialect='unix')
        writer.writeheader()

        for row in tqdm(reader, total=total, desc="Normalising Matilda IDs"):
            id_val = row['id']
            norm_id_val = normalise_value(id_val)
            row['id'] = norm_id_val
            writer.writerow(row)
            if id_val != norm_id_val:
                changed_id_vals += 1


    print(f"Metadata with normalised IDs (all lowercase) written to {out_path}")
    print(f"Number of IDs changed: {changed_id_vals}")
    return None

def validate(norm_fp, out_dir, use_lmdb=True):
    """
    Wrapper for calling Validator on the normalised metadata table.
    The existence of IDs is not verified (no calls to external APIs
    nor to OC SPARQL endopoint).
    Uses LMDB for validation to handle large datasets efficiently: if 
    error occurs due to disk space, set use_lmdb to False to use in-memory 
    validation instead (not recommended for large datasets).

    :param norm_fp: path to the normalised metadata CSV file
    :param out_dir: directory where the validation report will be written
    :param use_lmdb: whether to use LMDB for validation (default: True)
    """
    v = Validator(
        norm_fp,
        out_dir,
        verify_id_existence=False,
        use_lmdb=use_lmdb,
        map_size=30 *1024**3, # 30GB
    )
    report_fp = v.output_fp_json
    outcome = v.validate()
    print(f"Validation report written to {report_fp}")
    return (outcome, report_fp)

def prune_metadata(norm_fp, report_path, output_path):
    """
    Wrapper for calling oc_pruner.prune on the normalised metadata table 
    and the validation report.
    NOTE: the pruner is configured to ignore warnings and only remove rows with ERRORS.
    """
    # Create configuration
    config = PrunerConfig(
        error_type_filter="error", # only remove rows with ERRORS, keep rows with warnings
        ignore_error_labels=[] # if necessary, add to this list specific issues to ignore
    )

    # Prune the CSV file
    prune(
        csv_path=norm_fp,
        report_path=report_path,
        output_path=output_path,
        config=config,
        verbose=True
    )

    print(f"Pruned metadata written to {output_path}")
    return None

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", "-i", required=True, help="Path to input CSV file (metadata)")
    parser.add_argument("--output", "-o", required=True, help="Path to output CSV file (normalised and cleaned metadata)")
    parser.add_argument("--total", "-t", type=int, default=None, help="Total number of rows in input file for progress bar")

    args = parser.parse_args()
    output_dir = os.path.dirname(args.output)
    os.makedirs(output_dir, exist_ok=True)
    tmp_output_dir = tempfile.mkdtemp(dir=output_dir)
    original_fp = args.input
    normalised_fp = os.path.join(tmp_output_dir, "tmp_normalised_metadata.csv")
    if os.path.exists(normalised_fp):
        raise FileExistsError(f"Normalised metadata file already exists at {normalised_fp}, refusing to overwrite.")
    
    # NORMALISATION
    normalise_table(original_fp, normalised_fp, total=args.total)

    # VALIDATION
    is_valid, report_fp = validate(normalised_fp, tmp_output_dir)

    # PRUNING
    if is_valid:
        print("No errors found in the metadata, copying normalised file to output path.")
        os.replace(normalised_fp, args.output)
    else:
        prune_metadata(normalised_fp, report_fp, args.output)

    # Cleanup temporary files
    shutil.rmtree(tmp_output_dir)

if __name__ == "__main__":
    main()