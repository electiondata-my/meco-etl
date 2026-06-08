"""
Module: lake_results_headline.py

Writes headline election results to the data lake in CSV, Parquet, and Excel formats,
then uploads all files to R2 meco-lake.
It:
- Reads consolidated ballots and stats parquets from PATH_RESULTS_HEADLINE
- Writes sliced outputs (all, federal, by-elections, per-state state elections)
- Uploads all CSV, Parquet, and Excel outputs to R2 meco-lake

Inputs:
- PATH_RESULTS_HEADLINE/consol_ballots.parquet
- PATH_RESULTS_HEADLINE/consol_stats.parquet

Outputs:
- lake.electiondata.my/results_headline/headline_{v}[_{slice}].{csv,parquet,xlsx} uploaded to R2
"""

import os
from glob import glob as g
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv

from helper import write_csv_parquet_excel, get_states
from helper import get_r2_client, upload_bulk

load_dotenv()
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")
PATH_LOCAL_LAKE = "lake.electiondata.my/"


def make_headline_results():
    """Generate headline results CSV, Parquet, and Excel files for all slices."""
    for v in ["ballots", "stats"]:
        df = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}consol_{v}.parquet")
        write_csv_parquet_excel(f"{PATH_LOCAL_LAKE}results_headline/headline_{v}", df)
        write_csv_parquet_excel(
            f"{PATH_LOCAL_LAKE}results_headline/headline_{v}_federal",
            df[df.election.str.startswith("GE-")],
        )
        write_csv_parquet_excel(
            f"{PATH_LOCAL_LAKE}results_headline/headline_{v}_byelections",
            df[df.election.str.startswith("BY-")],
        )

        for state, state_code in zip(get_states(my=0, codes=0), get_states(my=0, codes=1)):
            if "W.P." in state:
                continue
            write_csv_parquet_excel(
                f"{PATH_LOCAL_LAKE}results_headline/headline_{v}_state_{state_code.lower()}",
                df[(df.state == state) & (df.election.str.startswith("SE-"))],
            )


def upload_headline_results(client, bucket):
    """Upload all headline results files to R2 meco-lake."""
    prefix = f"{PATH_LOCAL_LAKE}results_headline/"

    for ext, content_type in [
        ("csv", "text/csv"),
        ("parquet", "application/octet-stream"),
        ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    ]:
        files = sorted(g(f"{prefix}*.{ext}"))
        print(f"\nUploading {len(files):,.0f} {ext.upper()} files to R2")
        files_to_upload = [(f, f.replace(PATH_LOCAL_LAKE, "")) for f in files]
        upload_bulk(client, bucket, files_to_upload, content_type=content_type)


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET_LAKE = os.getenv("R2_BUCKET_LAKE")

    make_headline_results()
    upload_headline_results(CLIENT, BUCKET_LAKE)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"Duration: {datetime.now() - START}")
