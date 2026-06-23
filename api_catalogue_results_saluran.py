"""
Module: api_catalogue_results_saluran.py

Generates CSV/Excel exports and catalogue JSON files for saluran-level results datasets.
It:
- Reads all saluran parquet files and writes CSV and Excel versions
- Uploads all saluran files (parquet, csv, xlsx) to the lake R2 bucket
- Reads catalogue index to get all Saluran-Level dataset entries
- Fills saluran-ballots or saluran-stats template per entry
- Uploads all catalogue JSONs to the internal R2 bucket and purges the CF cache

Inputs:
- internal.electiondata.my/catalogue/index.json
- template-catalogue/saluran-ballots.json
- template-catalogue/saluran-stats.json
- lake.electiondata.my/results_saluran/*.parquet

Outputs:
- lake.electiondata.my/results_saluran/*.{csv,xlsx} uploaded to R2
- internal.electiondata.my/catalogue/results_saluran/{id}.json uploaded to R2
"""

import os
import json
from pathlib import Path
from datetime import datetime
from glob import glob as g
import pandas as pd
from dotenv import load_dotenv
from helper import get_r2_client, upload_bulk, purge_cf_cache_prefix

load_dotenv()

PATH_LOCAL_INTERNAL = "internal.electiondata.my/"
PATH_LOCAL_LAKE = "lake.electiondata.my/"
TEMPLATE_DIR = Path("template-catalogue")
RESULTS_DIR = Path(PATH_LOCAL_LAKE) / "results_saluran"

FORMAT_EXTS = {"parquet": ".parquet", "csv": ".csv"}

SALURAN_LEVELS = [
    "Saluran-Level (Federal)",
    "Saluran-Level (N9 State)",
    "Saluran-Level (Johor State)",
]


def _id_to_stem(entry_id):
    # saluran-ballots-ge15     → ge15_ballots
    # saluran-ballots-nsn-se15 → nsn_se15_ballots
    parts = entry_id.split("-")
    type_part = parts[1]
    election_parts = parts[2:]
    return "_".join(election_parts) + "_" + type_part


def save_csv():
    """Write CSV versions of every saluran parquet file."""
    parquets = sorted(RESULTS_DIR.glob("*.parquet"))
    print(f"\nWriting CSV for {len(parquets)} parquet files")
    for pq in parquets:
        df = pd.read_parquet(pq)
        out = RESULTS_DIR / f"{pq.stem}.csv"
        df.to_csv(out, index=False)
        print(f"Wrote: {out}")


def upload_saluran_files(client, bucket):
    """Upload all saluran parquet, CSV, and Excel files to the lake R2 bucket."""
    files = g(f"{PATH_LOCAL_LAKE}results_saluran/*")
    print(f"\nUploading {len(files):,.0f} files to lake R2")
    files_to_upload = sorted([(f, f.replace(PATH_LOCAL_LAKE, "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=60)


def _build_catalogue(template, entry):
    """Return a filled-in catalogue dict for a single results_saluran entry."""
    stem = _id_to_stem(entry["id"])

    df = pd.read_parquet(RESULTS_DIR / f"{stem}.parquet")
    n_rows = len(df)
    n_cols = len(df.columns)

    df_sample = df.head(30)
    if "date" in df_sample.columns:
        df_sample["date"] = pd.to_datetime(df_sample["date"]).dt.strftime("%Y-%m-%d")
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

    return cat


def make_results_saluran():
    """Generate catalogue JSONs for all Saluran-Level results_saluran entries."""
    index = json.loads(
        Path(f"{PATH_LOCAL_INTERNAL}catalogue/index.json").read_text(encoding="utf-8")
    )

    entries = []
    for level in SALURAN_LEVELS:
        entries.extend(index["data"]["Results"].get(level, []))

    templates = {
        "ballots": json.loads((TEMPLATE_DIR / "saluran-ballots.json").read_text()),
        "stats": json.loads((TEMPLATE_DIR / "saluran-stats.json").read_text()),
    }

    out_dir = Path(PATH_LOCAL_INTERNAL) / "catalogue" / "results_saluran"
    out_dir.mkdir(parents=True, exist_ok=True)

    for entry in entries:
        template_key = "stats" if "stats" in entry["id"] else "ballots"
        cat = _build_catalogue(templates[template_key], entry)
        out_path = out_dir / f"{entry['id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cat, f, ensure_ascii=False)
        print(f"Written: {out_path}")


def upload_catalogue_results_saluran(client, bucket):
    """Upload catalogue results_saluran JSONs to the internal R2 bucket."""
    files = g(f"{PATH_LOCAL_INTERNAL}catalogue/results_saluran/*.json")
    print(f"\nUploading {len(files):,.0f} catalogue files to R2")
    files_to_upload = sorted(
        [(f, f.replace(PATH_LOCAL_INTERNAL, "").replace("/results_saluran", "")) for f in files]
    )
    upload_bulk(client, bucket, files_to_upload, max_workers=60)


def purge_catalogue_results_saluran_cache(prefix="catalogue/"):
    """Purge Cloudflare cache for catalogue results_saluran JSONs by URL prefix."""
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

    # Task 2: Upload all saluran files to lake R2
    upload_saluran_files(CLIENT_LAKE, BUCKET_LAKE)

    # Task 3: Generate catalogue JSONs and upload to internal R2
    make_results_saluran()
    upload_catalogue_results_saluran(CLIENT_INTERNAL, BUCKET_INTERNAL)
    purge_catalogue_results_saluran_cache()

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
