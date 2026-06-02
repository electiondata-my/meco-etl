"""
Module: internal_results.py

This module processes and uploads election results data for internal.electiondata.my.
It:
- Reads consolidated candidate/results data and splits it into per-seat, per-date JSON files
- Uploads processed outputs to R2
- Purges Cloudflare cache for results files

Inputs:
- internal.electiondata.my/candidates.parquet (created by internal_candidates.py)

Outputs:
- internal.electiondata.my/results/{seat}/{date}.json uploaded to R2
"""

import os
import json as j
from glob import glob as g
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv

from helper import get_r2_client, upload_bulk, copy_bulk_within_r2, purge_cf_cache_prefix

load_dotenv()

PATH_LOCAL_INTERNAL = "internal.electiondata.my/"


def make_results():
    """Generate per-seat, per-date result JSON files and write them to disk."""
    print("")
    data = {"ballot": [], "summary": []}

    # fmt: off
    col_api_ballot = ["name", "party_uid", "party", "coalition_uid", "coalition", "votes", "votes_perc", "result"]
    col_api_ballot_summary = ["date", "voter_turnout", "voter_turnout_perc", "votes_rejected", "votes_rejected_perc", "majority", "majority_perc"]
    # fmt: on

    df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}candidates.parquet")
    df.date = pd.to_datetime(df.date).dt.date.astype(str)
    print(f"{df.drop_duplicates(subset=['seat','date']).shape[0]:,.0f} results to create")
    for seat in df.seat.unique():
        if not os.path.exists(f"{PATH_LOCAL_INTERNAL}results/{seat}"):
            os.makedirs(f"{PATH_LOCAL_INTERNAL}results/{seat}")

        dfs = df[df.seat == seat].copy()
        for date in dfs.date.unique():
            dfse = (
                dfs[dfs.date == date]
                .copy()[col_api_ballot]
                .sort_values(by="votes", ascending=False)
            )
            dfse_b = dfs[dfs.date == date].copy()[col_api_ballot_summary].drop_duplicates()

            res = dfse.to_dict(orient="records")
            res = [
                {k: (None if pd.isna(v) else v) for k, v in record.items()} for record in res
            ]  # proper JSON null

            res_b = dfse_b.to_dict(orient="records")
            res_b = [
                {k: (None if pd.isna(v) else v) for k, v in record.items()} for record in res_b
            ]  # proper JSON null

            data["ballot"] = res
            data["summary"] = res_b
            with open(
                f"{PATH_LOCAL_INTERNAL}results/{seat}/{date}.json", "w", encoding="utf-8"
            ) as f:
                j.dump(data, f)


def upload_results_jsons(client, bucket, file_pattern="results/**/*"):
    """Upload results JSON files to R2."""
    files = g(f"{PATH_LOCAL_INTERNAL}{file_pattern}.json")
    print(f"\nUploading {len(files):,.0f} files to R2")
    files_to_upload = sorted([(f, f.replace(PATH_LOCAL_INTERNAL, "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def purge_results_cache(prefix="results/"):
    """Purge Cloudflare cache for results JSON files by URL prefix."""
    full_prefix = f"{PATH_LOCAL_INTERNAL}{prefix}"
    print(f"\nPurging cache prefix: {full_prefix}")
    purge_cf_cache_prefix([full_prefix])


def duplicate_for_api(client, source_bucket, dest_bucket, prefix="results/"):
    """Copy results files from the internal bucket into v1/ on the API bucket."""
    copy_bulk_within_r2(client, source_bucket, dest_bucket, prefix, dest_prefix=f"v1/{prefix}")


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET_INTERNAL = os.getenv("R2_BUCKET_INTERNAL")
    BUCKET_API = os.getenv("R2_BUCKET_API")

    make_results()
    upload_results_jsons(CLIENT, BUCKET_INTERNAL)
    purge_results_cache()
    duplicate_for_api(CLIENT, BUCKET_INTERNAL, BUCKET_API)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
