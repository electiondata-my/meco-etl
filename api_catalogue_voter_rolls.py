"""
Module: api_catalogue_voter_rolls.py

Generates CSV exports and catalogue JSON files for anonymised voter roll datasets.
It:
- Reads all voter roll parquet files and writes CSV versions
- Uploads all voter roll files (parquet, csv) to the lake R2 bucket
- Reads catalogue index to get all Voter Rolls (Anonymised) dataset entries
- Fills voter-roll-anonymised template per entry
- Uploads all catalogue JSONs to the internal R2 bucket and purges the CF cache

Inputs:
- internal.electiondata.my/catalogue/index.json
- template-catalogue/voter-roll-anonymised.json
- lake.electiondata.my/voter_rolls/*.parquet

Outputs:
- lake.electiondata.my/voter_rolls/*.csv uploaded to R2
- internal.electiondata.my/catalogue/voter_rolls/{id}.json uploaded to R2
"""

import os
import json
from pathlib import Path
from datetime import datetime
from glob import glob as g
import duckdb
import pandas as pd
from dotenv import load_dotenv
from helper import get_r2_client, upload_bulk, purge_cf_cache_prefix

load_dotenv()

PATH_LOCAL_INTERNAL = "internal.electiondata.my/"
PATH_LOCAL_LAKE = "lake.electiondata.my/"
TEMPLATE_DIR = Path("template-catalogue")
RESULTS_DIR = Path(PATH_LOCAL_LAKE) / "voter_rolls"

FORMAT_EXTS = {"parquet": ".parquet", "csv": ".csv"}

VR_LEVELS = [
    "Federal Elections",
    "N9 State Elections",
    "Johor State Elections",
]


def _id_to_stem(entry_id):
    # voter-roll-anonymised-ge15     → ge15_2022
    # voter-roll-anonymised-nsn-se15 → nsn_se15_2023
    # Strip prefix, then glob for matching file
    election = entry_id.replace("voter-roll-anonymised-", "").replace("-", "_")
    matches = list(RESULTS_DIR.glob(f"{election}_*.parquet"))
    if not matches:
        raise FileNotFoundError(f"No parquet found for election '{election}' in {RESULTS_DIR}")
    return matches[0].stem


def save_csv():
    """Write CSV versions of every voter roll parquet file using DuckDB COPY."""
    parquets = sorted(RESULTS_DIR.glob("*2026.parquet"))
    print(f"\nWriting CSV for {len(parquets)} parquet files")
    con = duckdb.connect()
    for pq in parquets:
        out = RESULTS_DIR / f"{pq.stem}.csv"
        con.execute(f"COPY (SELECT * FROM '{pq}') TO '{out}' (HEADER, DELIMITER ',')")
        print(f"Wrote: {out}")


def upload_voter_roll_files(client, bucket):
    """Upload all voter roll parquet and CSV files to the lake R2 bucket."""
    files = g(f"{PATH_LOCAL_LAKE}voter_rolls/*2026*")
    print(f"\nUploading {len(files):,.0f} files to lake R2")
    files_to_upload = sorted([(f, f.replace(PATH_LOCAL_LAKE, "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=10)


def _build_catalogue(template, entry):
    """Return a filled-in catalogue dict for a single voter roll entry."""
    stem = _id_to_stem(entry["id"])

    df = pd.read_parquet(RESULTS_DIR / f"{stem}.parquet")
    n_rows = len(df)
    n_cols = len(df.columns)

    df_sample = df.head(30)
    sample_rows = json.loads(df_sample.to_json(orient="records"))

    cat_str = json.dumps(template)
    cat_str = cat_str.replace("IDENTIFIER", stem)
    cat_str = cat_str.replace("TITLE", entry["title"])
    cat_str = cat_str.replace("DESCRIPTION.", entry["description"])
    cat = json.loads(cat_str)

    cat["data_as_of"] = entry["data_as_of"]

    for fmt, ext in FORMAT_EXTS.items():
        fp = RESULTS_DIR / (stem + ext)
        cat["download"][fmt]["n_rows"] = n_rows
        cat["download"][fmt]["n_cols"] = n_cols
        if fp.exists():
            cat["download"][fmt]["size_bytes"] = fp.stat().st_size

    cat["sample_data"] = sample_rows

    if stem.startswith("ge12_"):
        cat["fields"].append(
            {
                "name": "do_not_link",
                "title": "Do Not Link",
                "description": "[Integer] 1 if a valid IC number could not be obtained for this row, meaning it cannot be linked to subsequent voter rolls; 0 otherwise",
            }
        )

    return cat


def make_voter_rolls():
    """Generate catalogue JSONs for all Voter Rolls (Anonymised) entries."""
    index = json.loads(
        Path(f"{PATH_LOCAL_INTERNAL}catalogue/index.json").read_text(encoding="utf-8")
    )

    entries = []
    for level in VR_LEVELS:
        entries.extend(index["data"]["Voter Rolls (Anonymised)"].get(level, []))

    template = json.loads((TEMPLATE_DIR / "voter-roll-anonymised.json").read_text())

    out_dir = Path(PATH_LOCAL_INTERNAL) / "catalogue" / "voter_rolls"
    out_dir.mkdir(parents=True, exist_ok=True)

    for entry in entries:
        cat = _build_catalogue(template, entry)
        out_path = out_dir / f"{entry['id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cat, f, ensure_ascii=False)
        print(f"Written: {out_path}")


def upload_catalogue_voter_rolls(client, bucket):
    """Upload catalogue voter roll JSONs to the internal R2 bucket."""
    files = g(f"{PATH_LOCAL_INTERNAL}catalogue/voter_rolls/*.json")
    print(f"\nUploading {len(files):,.0f} catalogue files to R2")
    files_to_upload = sorted(
        [(f, f.replace(PATH_LOCAL_INTERNAL, "").replace("/voter_rolls", "")) for f in files]
    )
    upload_bulk(client, bucket, files_to_upload, max_workers=10)


def purge_catalogue_voter_rolls_cache(prefix="catalogue/"):
    """Purge Cloudflare cache for catalogue voter roll JSONs by URL prefix."""
    full_prefix = f"{PATH_LOCAL_INTERNAL}{prefix}"
    print(f"\nPurging cache prefix: {full_prefix}")
    purge_cf_cache_prefix([full_prefix])


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT_LAKE = get_r2_client()
    BUCKET_LAKE = os.getenv("R2_BUCKET_LAKE")
    CLIENT_INTERNAL = get_r2_client()
    BUCKET_INTERNAL = os.getenv("R2_BUCKET_INTERNAL")

    # Task 1: Save CSV
    save_csv()

    # Task 2: Upload all voter roll files to lake R2
    upload_voter_roll_files(CLIENT_LAKE, BUCKET_LAKE)

    # Task 3: Generate catalogue JSONs and upload to internal R2
    make_voter_rolls()
    upload_catalogue_voter_rolls(CLIENT_INTERNAL, BUCKET_INTERNAL)
    purge_catalogue_voter_rolls_cache()

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
