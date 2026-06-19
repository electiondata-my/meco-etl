"""
Module: api_candidates.py

Processes and uploads candidate data for the public API at api.electiondata.my.
It:
- Reads and transforms candidate data from the internal parquet
- Generates a dropdown JSON (with slug renamed to uid)
- Generates per-candidate JSONs with seat split into seat and state
- Uploads all outputs to the meco-api R2 bucket

Inputs:
- internal.electiondata.my/candidates.parquet
- internal.electiondata.my/candidates/dropdown.json
- internal.electiondata.my/candidates/all.json

Outputs:
- api.electiondata.my/v1/candidates/dropdown.json uploaded to R2
- api.electiondata.my/v1/candidates/{uid}.json uploaded to R2
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
PATH_LOCAL_API = "api.electiondata.my/v1/candidates/"


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


def make_api_candidates_jsons():
    """Generate candidate JSON files for the public API.

    Inputs:
    - internal.electiondata.my/candidates/dropdown.json
    - internal.electiondata.my/candidates/all.json

    Outputs:
    - api.electiondata.my/v1/candidates/dropdown.json
    - api.electiondata.my/v1/candidates/{uid}.json
    """
    os.makedirs(PATH_LOCAL_API, exist_ok=True)

    # Dropdown: same as internal but with slug renamed to uid
    with open(f"{PATH_LOCAL_INTERNAL}candidates/dropdown.json", encoding="utf-8") as f:
        dropdown = j.load(f)
    dropdown["data"] = [{"uid": r.pop("slug"), **r} if "slug" in r else r for r in dropdown["data"]]
    with open(f"{PATH_LOCAL_API}dropdown.json", "w", encoding="utf-8") as f:
        j.dump({"candidates": dropdown["data"]}, f)
    print("Wrote api candidates/dropdown.json")

    # Individual candidate JSONs: seat field split into seat + state
    with open(f"{PATH_LOCAL_INTERNAL}candidates/all.json", encoding="utf-8") as f:
        all_data = j.load(f)

    for uid, contests in all_data.items():
        new_contests = []
        for contest in contests:
            seat_state = contest.get("seat")
            if not seat_state:
                new_contests.append(contest)
                continue
            parts = seat_state.rsplit(", ", 1)
            new_contest = {}
            for k, v in contest.items():
                new_contest[k] = v
                if k == "seat":
                    new_contest["seat"] = parts[0]
                    new_contest["state"] = parts[1] if len(parts) > 1 else None
            new_contests.append(new_contest)
        with open(f"{PATH_LOCAL_API}{uid}.json", "w", encoding="utf-8") as f:
            j.dump({"results": new_contests}, f)
    print(f"Wrote {len(all_data):,.0f} individual candidate JSONs")


def upload_api_candidates_jsons(client, bucket):
    """Upload API candidate JSON files to R2."""
    files = g(f"{PATH_LOCAL_API}*.json")
    print(f"\nUploading {len(files):,.0f} API candidate files to R2")
    files_to_upload = sorted([(f, f.replace("api.electiondata.my/", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET_INTERNAL = os.getenv("R2_BUCKET_INTERNAL")
    BUCKET_API = os.getenv("R2_BUCKET_API")

    make_candidates_df()
    make_candidates_jsons()
    upload_candidates_jsons(CLIENT, BUCKET_INTERNAL)
    purge_candidates_cache()

    make_api_candidates_jsons()
    upload_api_candidates_jsons(CLIENT, BUCKET_API)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
