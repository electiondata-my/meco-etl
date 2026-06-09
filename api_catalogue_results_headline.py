"""
Module: api_catalogue_results_headline.py

Generates catalogue JSON files for headline results datasets (ballots and stats)
by combining templates with real file sizes, row counts, and sample data.
It:
- Reads catalogue index to get all Constituency-Level dataset entries
- Reads local parquet files for file stats and sample data
- Fills headline-ballots or headline-stats template per entry
- Uploads all catalogue JSONs to R2 and purges the CF cache

Inputs:
- internal.electiondata.my/catalogue/index.json
- template-catalogue/headline-ballots.json
- template-catalogue/headline-stats.json
- lake.electiondata.my/results_headline/*.{parquet,csv,xlsx}

Outputs:
- api.electiondata.my/catalogue/results_headline/{id}.json
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
RESULTS_DIR = Path(PATH_LOCAL_LAKE) / "results_headline"

FORMAT_EXTS = {"parquet": ".parquet", "csv": ".csv", "excel": ".xlsx"}


def _id_to_stem(entry_id):
    return entry_id.replace("-", "_")


def _build_catalogue(template, entry):
    """Return a filled-in catalogue dict for a single results_headline entry."""
    stem = _id_to_stem(entry["id"])

    df = pd.read_parquet(RESULTS_DIR / f"{stem}.parquet")
    n_rows = len(df)
    n_cols = len(df.columns)

    df_sample = df.sort_values(["date", "state", "seat"], ascending=[False, True, True]).head(30)
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


def make_results_headline():
    """Generate catalogue JSONs for all Constituency-Level results_headline entries."""
    index = json.loads(
        Path(f"{PATH_LOCAL_INTERNAL}catalogue/index.json").read_text(encoding="utf-8")
    )
    entries = index["data"]["Results"]["Constituency-Level"]

    templates = {
        "ballots": json.loads((TEMPLATE_DIR / "headline-ballots.json").read_text()),
        "stats": json.loads((TEMPLATE_DIR / "headline-stats.json").read_text()),
    }

    out_dir = Path(PATH_LOCAL_INTERNAL) / "catalogue" / "results_headline"
    out_dir.mkdir(parents=True, exist_ok=True)

    for entry in entries:
        template_key = "stats" if "stats" in entry["id"] else "ballots"
        cat = _build_catalogue(templates[template_key], entry)
        out_path = out_dir / f"{entry['id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cat, f, ensure_ascii=False)
        print(f"Written: {out_path}")


def upload_catalogue_results_headline(client, bucket):
    """Upload catalogue results_headline JSONs to R2."""
    files = g(f"{PATH_LOCAL_INTERNAL}catalogue/results_headline/*.json")
    print(f"\nUploading {len(files):,.0f} files to R2")
    files_to_upload = sorted(
        [(f, f.replace(PATH_LOCAL_INTERNAL, "").replace("/results_headline", "")) for f in files]
    )
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def purge_catalogue_results_headline_cache(prefix="catalogue/"):
    """Purge Cloudflare cache for catalogue results_headline JSONs by URL prefix."""
    full_prefix = f"{PATH_LOCAL_INTERNAL}{prefix}"
    print(f"\nPurging cache prefix: {full_prefix}")
    purge_cf_cache_prefix([full_prefix])


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET = os.getenv("R2_BUCKET_INTERNAL")

    make_results_headline()
    upload_catalogue_results_headline(CLIENT, BUCKET)
    purge_catalogue_results_headline_cache()

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
