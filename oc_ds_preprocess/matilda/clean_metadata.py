from oc_validator.main import Validator
import csv
from csv import DictReader, DictWriter
from tqdm import tqdm
import sys
from pathlib import Path
import shutil
import json
import argparse


def get_all_ids(meta_fp) -> dict:
    """
    Return a dictionary mapping each ID in the metadata table to its row index. 
    This will be used to check which IDs are not associated with valid citations 
    in the citations table, and to write the final metadata table without those IDs.

    :param meta_fp: file path to the clean normalised metadata table.
    """
    csv.field_size_limit(sys.maxsize)
    ids_map = dict()
    with open(meta_fp, 'r', encoding='utf-8', newline='') as f:
        reader = DictReader(f)
        for idx, row in tqdm(enumerate(reader), desc='Loading IDs in metadata...'):
            for i in row['id'].split():
                ids_map[i] = idx  # metadata table contains no duplicates, so we can just store the last index for each ID.

    with open('ids_in_matilda_meta.jsonl', 'w', encoding='utf-8') as f:        
        for _id, row in ids_map.items():
            f.write(json.dumps({'id': _id, 'row_index': row}) + '\n')
    return ids_map


def get_missing_cits_ids(meta_ids:dict, cits_fps:list) -> dict:
    """
    Removes from the input dictionary of IDs those that are 
    associated with valid citations in the citations tables, 
    and returns the remaining IDs with their row indices in the metadata table.

    :param meta_ids: dictionary of the form `<id>: <row index>` storing the all 
        the IDs in the metadata table.
    :param cits_fps: list of file paths for all the tables storing VALID citations only
    :return: dict storing IDs that are NOT involved in any valid citation
    """
    csv.field_size_limit(sys.maxsize)

    remaining = dict(meta_ids)
    print('Initial number of IDs collected from metadata table:', len(remaining))
    for fp in cits_fps:
        with open(fp, 'r', encoding='utf-8', newline='') as f:
            reader = DictReader(f)
            for idx, row in tqdm(enumerate(reader), desc='Checking closure...'):
                ids = set(i for i in row['citing_id'].split() + row['cited_id'].split())
                for i in ids:
                    remaining.pop(i, None)
    
    count = len(remaining.keys())
    print(f'Number of IDs that are NOT associated with valid citations: {count}')
    if count:
        print('Writing IDs without valid citations associated to "ids_without_valid_cits.jsonl". ')
        with open('ids_without_valid_cits.jsonl', 'w', encoding='utf-8') as f:        
            for _id, row in remaining.items():
                f.write(json.dumps({'id': _id, 'row_index': row}) + '\n')
    return remaining

def remove_orphan_entities(meta_fp, cits_fps:list, final_meta_fp):
    """
    Main function to perform the closure operation on the metadata table: 
    removes from the metadata table those IDs that are not associated with 
    valid citations in the citations table, and writes the final metadata table 
    to a new CSV file.

    :param meta_fp: file path to the clean normalised metadata table 
        (potentially containing entities that are not involved in valid citations)
    
    :param cits_fps: list of file paths to the tables containing only valid citations.
    """
    csv.field_size_limit(sys.maxsize)

    meta_fp = Path(meta_fp)
    cits_fps = [Path(f) for f in cits_fps]

    all_ids = get_all_ids(meta_fp)
    invalid = get_missing_cits_ids(all_ids, cits_fps)
    invalid_row_indices = set(invalid.values())

    final_meta_fp = Path(final_meta_fp)
    print(f'Writing final metadata to {final_meta_fp}...')
    with open(final_meta_fp, 'w', encoding='utf-8', newline='') as fout, open(meta_fp, 'r', encoding='utf-8') as fin:
        reader = DictReader(fin)
        writer = DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()
        for idx, row in tqdm(enumerate(reader),  desc='Writing final metadata...'):
            if idx not in invalid_row_indices:
                writer.writerow(row)
    
    print('---- Done.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Remove metadata entries (orphans) that do not appear in any valid citation file as citing or cited entities."
    )

    # Required: The source metadata CSV
    parser.add_argument(
        "--meta", 
        type=str, 
        required=True, 
        help="Path to the input metadata CSV file."
    )

    # Required: One or more citation CSV files (nargs='+' allows multiple paths)
    parser.add_argument(
        "--cits", 
        type=str, 
        nargs="+", 
        required=True, 
        help="One or more paths to citation CSV files (separated by space)."
    )

    # Required: The destination for the filtered metadata
    parser.add_argument(
        "--out", 
        type=str, 
        required=True, 
        help="Path where the final filtered metadata CSV will be saved."
    )

    args = parser.parse_args()

    # Call the main logic function with the parsed arguments
    remove_orphan_entities(
        meta_fp=args.meta, 
        cits_fps=args.cits, 
        final_meta_fp=args.out
    )