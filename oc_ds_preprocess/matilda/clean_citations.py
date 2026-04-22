from oc_pruner import prune
from oc_pruner.config import PrunerConfig
from oc_validator.main import Validator
import tempfile
import shutil
from pathlib import Path
import os
from datetime import date
import shutil
import argparse

def clean_cit_tables(input_dir, output_dir, use_lmdb=True):
    """
    Validates the citations tables in input_dir and generates a new copy with only valid rows. 
    The validation is performed using oc_validator, and the pruning of invalid rows is done using oc_pruner. 
    Validation reports are generated in a temporary directory and deleted at the end of the process. 
    Validation can make use of LMDB caches. If available RAM is limited, set use_lmdb to 
    True in the Validator configuration to use file-based caching instead (requires sufficient disk space).
    The pruner is configured to ignore warnings and only remove rows with ERRORS.


    :param input_dir: path to the directory containing the citations tables to be cleaned.
    :param output_dir: path to the directory where the cleaned citations tables will be written.
    :param use_lmdb: whether to use LMDB for validation (default: False)
    """

    conf = PrunerConfig(
        error_type_filter='error' # only remove rows with ERRORS, keep rows with warnings
    )

    input_dir = Path(input_dir)
    tables_fps = [fp for fp in input_dir.glob("*.csv")]
    print(f"Found {len(tables_fps)} tables to validate and prune.")

    outdir_path = Path(output_dir, f"cleaned_cits_{date.today().isoformat()}")
    os.makedirs(outdir_path, exist_ok=True)

    tmp_reports_dir = Path(tempfile.mkdtemp(prefix='tmp_reports', dir=output_dir))
    for table_fp in tables_fps:
        # check if the file has only the header row
        # in which case we can skip validation and just copy it to the output directory
        with open(table_fp, 'r', encoding='utf-8') as f:
            try:
                first_lines = [next(f) for _ in range(2)]
            except StopIteration:
                first_lines = []
            if len(first_lines) < 2:
                print(f"{table_fp} has only header row, copying to output directory without validation.")
                os.replace(table_fp, outdir_path / table_fp.name)
                continue

        print(f"Validating {table_fp}...")
        
        # Validate the citations table
        v = Validator(
            csv_doc=table_fp,
            output_dir=Path(tmp_reports_dir, table_fp.stem),
            verify_id_existence=False,
            use_lmdb=use_lmdb,
            map_size=30 *1024**3, # 30GB,
            cache_dir=Path(tmp_reports_dir, 'cache')
        )
        is_valid = v.validate()
        if is_valid:
            print(f"No issues found in {table_fp}, copying to output directory.")
            os.replace(table_fp, outdir_path / table_fp.name)
        else:
            print(f"Issues found in {table_fp}, pruning...")

            # Prune the citations table based on the validation report
            prune(
                csv_path=table_fp,
                report_path=v.output_fp_json,
                output_path=outdir_path / table_fp.name,
                config=conf,
                verbose=False
            )
            print(f"Pruned {table_fp} written to {outdir_path / table_fp.name}")
    print(f"---- All tables processed. Cleaned citations tables are available at {outdir_path}")
    shutil.rmtree(tmp_reports_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate and prune citations tables in a directory.")
    parser.add_argument("--input_dir", "-i", required=True, help="Path to the directory containing the citations tables to be cleaned.")
    parser.add_argument("--output_dir", "-o", required=True, help="Path to the directory where the cleaned citations tables will be written.")
    parser.add_argument("--use_lmdb", action='store_true', help="Whether to use LMDB for validation (default: False). If available RAM is limited, set this flag to use file-based caching instead (requires sufficient disk space).")
    
    args = parser.parse_args()
    clean_cit_tables(args.input_dir, args.output_dir, use_lmdb=args.use_lmdb)