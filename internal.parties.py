"""
Module: internal_parties.py


"""

import os
import json as j
from glob import glob as g
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv

from helper import write_parquet, get_r2_client, upload_bulk

load_dotenv()
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")
PATH_LOCAL_INTERNAL = "internal.electiondata.my/"


def make_parties_df():
    """
    This function:
    - Reads and processes candidate data
    - Drops unnecessary columns
    - Filters out By-Elections (retaining only general/state elections)
    - Aggregates results by party and coalition
    - Calculates total seats/votes and their percentages per election and state
    - Handles special cases such as uncontested seats
    - Writes final party-level and coalition-level aggregates to parquet files

    Inputs:
    - internal.electiondata.my/candidates.parquet
    -  PATH_RESULTS_HEADLINE/lookup_dates.parquet
    -  PATH_RESULTS_HEADLINE/lookup_party.parquet

    Outputs:
    - internal.electiondata.my/parties.parquet (results grouped by party)
    - internal.electiondata.my/coalitions.parquet (results grouped by coalition)

    Returns:
        None
    """

    ds = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}lookup_dates.parquet")
    ds.election_number = "GE-" + ds.election_number.astype(str).str.zfill(2)
    ds.loc[ds.state != "Malaysia", "election_number"] = ds.election_number.str.replace("GE-", "SE-")
    ds = ds.rename(columns={"election_number": "election_name"})
    map_ge_date = dict(zip(ds.election_name, ds.date))

    pf = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}lookup_party.parquet")
    pf["uid"] = pf.party_uid.str.split("-").str[0]
    pf = pf[["uid", "party_uid", "party"]].drop_duplicates(subset=["uid"], keep="last")
    map_uid_party_uid = dict(zip(pf.uid, pf.party_uid))
    map_uid_party_acronym = dict(zip(pf.party_uid, pf.party))

    col_idx = [
        "party_uid",
        "party",
        "coalition",
        "coalition_uid",
        "type",
        "state",
        "election_name",
        "date",
    ]
    df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}candidates.parquet")
    df["party_uid"] = df.party_uid.str.split("-").str[0].map(map_uid_party_uid)
    df["party"] = df.party_uid.map(map_uid_party_acronym)
    df = df[df.election_name != "By-Election"]  # Remove By-Elections, we are not interested in them
    df = df.drop(
        [
            "seat",
            "date",
            "voter_turnout",
            "voter_turnout_perc",
            "votes_rejected",
            "votes_rejected_perc",
            "majority",
            "majority_perc",
        ],
        axis=1,
    )
    df["seats_contested"] = 1
    df["seats_won"] = 0
    df.loc[df.result.str.contains("won"), "seats_won"] = 1
    df = pd.merge(df, ds, on=["state", "election_name"], how="left")
    df.loc[df.election_name.str.contains("GE-"), "date"] = df.election_name.map(map_ge_date)
    df = pd.concat(
        [df[df.election_name.str.contains("GE-")].assign(state="Malaysia"), df],
        axis=0,
        ignore_index=True,
    )
    df = (
        df.drop(["name", "votes_perc", "result", "slug"], axis=1)
        .groupby(col_idx)
        .sum()
        .reset_index()
    )

    # add total number of seats and votes per election (sf), then compute percentages
    col_idx_sf = ["election_name", "state"]
    sf = df[col_idx_sf + ["votes", "seats_won"]].copy().groupby(col_idx_sf).sum().reset_index()
    sf.columns = col_idx_sf + ["votes_total", "seats_total"]
    df = pd.merge(df, sf, on=col_idx_sf, how="left")
    df["votes_perc"] = df.votes / df.votes_total * 100
    df["seats_contested_perc"] = df.seats_contested / df.seats_total * 100
    df["seats_won_perc"] = df.seats_won / df.seats_total * 100
    df.loc[(df.election_name == "SE-02") & (df.state == "Sabah"), "votes_perc"] = (
        df.seats_contested_perc
    )  # special case where all seats were uncontested
    df = df[
        col_idx
        + ["seats_contested", "seats_won", "seats_total", "seats_contested_perc", "seats_won_perc"]
        + ["votes", "votes_total", "votes_perc"]
    ]

    df[(df.election_name == "SE-02") & (df.state == "Sabah")].sort_values(
        by="seats_won", ascending=False
    )
    print(f"\n{len(df.party_uid.unique()):,.0f} unique parties")
    print(f"{len(df.coalition_uid.unique()):,.0f} unique coalitions")
    write_parquet(f"{PATH_LOCAL_INTERNAL}parties", df)
    df_party = df.copy()

    df = df.drop(columns=["party_uid", "party"])
    df = df.groupby(col_idx[2:]).sum().reset_index()
    df.seats_total = ((df.seats_contested * 100) / (df.seats_contested_perc)).round(0).astype(int)
    df.votes_total = ((df.votes * 100) / (df.votes_perc)).round(0).astype(int)
    for c in ["votes_total", "seats_total"]:
        assert len(df.drop_duplicates(subset=["election_name", "state"])) == len(
            df.drop_duplicates(subset=["election_name", "state", c])
        )
    write_parquet(f"{PATH_LOCAL_INTERNAL}coalitions", df)

    return df_party, df


def make_parties_jsons():
    """Generate party data files for API."""

    # -------- dropdown --------
    data = {"data": []}

    df = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}consol_ballots.parquet")
    parties_contested = list(df.party_uid.unique())

    df = pd.read_parquet(
        f"{PATH_RESULTS_HEADLINE}lookup_party.parquet",
        columns=["party_uid", "party", "party_name_en", "party_name_bm"],
    )
    df = df[df.party_uid.isin(parties_contested)]
    tf = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}lookup_coalition.parquet")
    tf = tf[tf.coalition_uid != 0]
    tf.coalition_uid = tf.coalition_uid.astype(str).str.zfill(2) + "-" + tf.coalition
    tf.columns = [x.replace("coalition", "party") for x in tf.columns]
    df = pd.concat(
        [
            df.assign(type="party").sort_values(by="party"),
            tf.assign(type="coalition").sort_values(by="party"),
        ],
        axis=0,
        ignore_index=True,
    )
    df = df.to_dict(orient="records")

    data["data"] = df

    with open(f"{PATH_LOCAL_INTERNAL}parties/dropdown.json", "w", encoding="utf-8") as f:
        j.dump(data, f, ensure_ascii=False)

    # -------- all.json --------
    col_party = [
        "state",
        "type",
        "coalition",
        "coalition_uid",
        "election_name",
        "date",
        "seats_contested",
        "seats_won",
        "seats_total",
        "seats_contested_perc",
        "seats_won_perc",
        "votes",
        "votes_perc",
    ]

    df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}parties.parquet")
    df.coalition_uid = df.coalition_uid.astype(str).str.zfill(2) + "-" + df.coalition
    tf = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}coalitions.parquet").rename(
        columns={"coalition_uid": "party_uid", "coalition": "party"}
    )
    tf.party_uid = tf.party_uid.astype(str).str.zfill(2) + "-" + tf.party
    print(
        f"\nHandling {len(df.party_uid.unique()):,.0f} unique parties and {len(tf.party_uid.unique()):,.0f} unique coalitions"
    )
    tf["coalition"] = tf["coalition_uid"] = "-"
    df = pd.concat(
        [df.assign(party_type="party"), tf.assign(party_type="coalition")],
        axis=0,
        ignore_index=True,
    )
    df.date = pd.to_datetime(df.date).dt.strftime("%Y-%m-%d")

    df = df[["party_uid", "party_type"] + col_party].sort_values(by="date", ascending=False)
    df = df.astype(object).where(df.notna(), other=None)  # proper JSON null

    all_data = {
        party_uid: group.drop(columns="party_uid").to_dict(orient="records")
        for party_uid, group in df.groupby("party_uid", sort=True)
    }

    with open(f"{PATH_LOCAL_INTERNAL}parties/all.json", "w", encoding="utf-8") as f:
        j.dump(all_data, f)
        print("Wrote parties/all.json")


def upload_parties_jsons(client, bucket, file_pattern="parties/*"):
    """Upload data files matching pattern to R2."""
    files = g(f"{PATH_LOCAL_INTERNAL}{file_pattern}.json")
    print(f"\nUploading {len(files):,.0f} files to R2")
    files_to_upload = sorted([(f, f.replace(PATH_LOCAL_INTERNAL, "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET = os.getenv("R2_BUCKET_INTERNAL")

    make_parties_df()
    make_parties_jsons()
    upload_parties_jsons(CLIENT, BUCKET)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
