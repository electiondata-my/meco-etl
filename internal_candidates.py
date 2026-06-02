"""
Module: internal_candidates.py

This module processes and uploads candidate and election results data for internal.electiondata.my.
It:
- Reads, merges, and transforms data from headline results files
- Prepares consolidated candidate DataFrames
- Uploads processed outputs to R2

Inputs:
-  PATH_RESULTS_HEADLINE/consol_ballots.parquet
-  PATH_RESULTS_HEADLINE/consol_stats.parquet

Outputs:
- internal.electiondata.my/candidates.parquet
- Processed candidate-level tables uploaded to R2 in minified JSON format
"""

import os
import json as j
from glob import glob as g
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv

from helper import write_parquet, get_r2_client, upload_bulk, purge_cf_cache_prefix

load_dotenv()
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")
PATH_LOCAL_INTERNAL = "internal.electiondata.my/"


def make_candidates_df():
    """
    This function:
    - Reads and wrangles consolidated ballots file
    - Reads consolidated stats file, and merges it onto the ballots (join on contest identifiers)
    - Returns the combined result as a pandas DataFrame

    Inputs (cleaned and validated):
    - {PATH_RESULTS_HEADLINE}consol_ballots.parquet
    - {PATH_RESULTS_HEADLINE}consol_stats.parquet

    Outputs:
    - internal.electiondata.my/candidates.parquet

    Returns:
        None
    """
    # fmt: off
    col_join = ["date", "election_name", "state", "seat"]
    col_ballot = [
        "slug", "name", "type",
    ] + col_join + [
        "party", "party_uid","coalition","coalition_uid",
        "votes", "votes_perc", "result",
    ]
    col_stats = col_join + [
        "voters_total", "n_candidates",
        "voter_turnout", "voter_turnout_perc", 
        "votes_rejected", "votes_rejected_perc", 
        "majority", "majority_perc",
        "ballots_not_returned", "ballots_not_returned_perc",
    ]
    # fmt: on

    df = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}consol_ballots.parquet").rename(
        columns={"candidate_uid": "slug", "election": "election_name"}
    )
    df["type"] = "parlimen"
    df.loc[df.seat.str.startswith("N."), "type"] = "dun"
    df.seat = df.seat + ", " + df.state
    df = df[col_ballot]

    sf = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}consol_stats.parquet").rename(
        columns={
            "voter_turnout": "voter_turnout_perc",
            "ballots_issued": "voter_turnout",
            "election": "election_name",
        }
    )
    sf.seat = sf.seat + ", " + sf.state
    sf = sf[col_stats]

    df = pd.merge(df, sf, on=["date", "election_name", "state", "seat"], how="left")
    df.election_name = df.election_name.replace("BY-ELECTION", "By-Election")
    print(f"\nDataframe with {len(df.slug.unique()):,.0f} unique candidate produced")
    write_parquet(f"{PATH_LOCAL_INTERNAL}candidates", df)


def make_candidates_jsons():
    """Generate candidate data files for internal site operations.

    Inputs:
    - pandas.DataFrame: DataFrame with candidates data

    Outputs:
    - internal.electiondata.my/candidates/dropdown.json
    - internal.electiondata.my/candidates/all.json

    Returns:
        None
    """
    data = {"data": []}
    candidates_df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}candidates.parquet")

    print(f"\nHandling {len(candidates_df.slug.unique()):,.0f} unique candidates")
    df = candidates_df.copy()
    df = (
        df.assign(c=1, w=df.result.str.contains("won").astype(int), l=lambda x: 1 - x.w)
        .groupby(["slug", "name"], as_index=False)
        .agg({"c": "sum", "w": "sum", "l": "sum"})
        .sort_values(["c", "w"], ascending=False)
    )
    df = df.to_dict(orient="records")
    df = [
        {k: (None if pd.isna(v) else v) for k, v in record.items()} for record in df
    ]  # proper JSON null
    data["data"] = df
    with open(f"{PATH_LOCAL_INTERNAL}candidates/dropdown.json", "w", encoding="utf-8") as f:
        j.dump(data, f)
        print("Wrote candidates/dropdown.json")

    col_api_candidate = [
        "name",
        "election_name",
        "type",
        "date",
        "seat",
        "party",
        "party_uid",
        "coalition",
        "coalition_uid",
        "votes",
        "votes_perc",
        "result",
    ]

    df = candidates_df.copy()
    df.date = pd.to_datetime(df.date).dt.strftime("%Y-%m-%d")

    df = df[col_api_candidate + ["slug"]].sort_values(by="date", ascending=False)
    df = df.astype(object).where(df.notna(), other=None)  # proper JSON null

    all_data = {
        slug: group.drop(columns="slug").to_dict(orient="records")
        for slug, group in df.groupby("slug", sort=True)
    }

    with open(f"{PATH_LOCAL_INTERNAL}candidates/all.json", "w", encoding="utf-8") as f:
        j.dump(all_data, f)
        print("Wrote candidates/all.json")


def upload_candidates_jsons(client, bucket, file_pattern="candidates/*"):
    """Upload data files matching pattern to R2."""
    files = g(f"{PATH_LOCAL_INTERNAL}{file_pattern}.json")
    print(f"\nUploading {len(files):,.0f} files to R2")
    files_to_upload = sorted([(f, f.replace(PATH_LOCAL_INTERNAL, "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def purge_candidates_cache(prefix="candidates/"):
    """Purge Cloudflare cache for candidate JSON files by URL prefix."""
    full_prefix = f"{PATH_LOCAL_INTERNAL}{prefix}"
    print(f"\nPurging cache prefix: {full_prefix}")
    purge_cf_cache_prefix([full_prefix])


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET = os.getenv("R2_BUCKET_INTERNAL")

    make_candidates_df()
    make_candidates_jsons()
    upload_candidates_jsons(CLIENT, BUCKET)
    purge_candidates_cache()

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
