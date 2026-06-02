"""
Module: internal_elections.py
"""

import os
import json as j
from glob import glob as g
from datetime import datetime
import pandas as pd
import duckdb

from dotenv import load_dotenv

from helper import write_parquet, generate_slug
from helper import get_r2_client, upload_bulk, purge_cf_cache_prefix

load_dotenv()
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")
PATH_LOCAL_INTERNAL = "internal.electiondata.my/"


def make_election_stats():
    """
    The function:
        Reads election summary data from consol_summary.parquet,
        then filters out By-Elections,
        then filters out uncontested wins,
        then groups by state and type,
        then calculates voter turnout and rejected votes percentages,
        then writes final dataframe to parquet format.
    Inputs:
        internal.electiondata.my/candidates.parquet
    Outputs:
        internal.electiondata.my/elections_stats.parquet
    """
    # fmt: off
    col_idx = ["type","election_name","state"]
    col_summary = [
        "voters_total", "n_candidates",
        "voter_turnout", "voter_turnout_perc", 
        "votes_valid", "votes_rejected", "votes_rejected_perc", 
    ]
    # fmt: on

    df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}candidates.parquet")
    df["votes_valid"] = df.voter_turnout - df.ballots_not_returned
    df["voters_contested"] = df["voters_total"]
    df.loc[df.result.str.contains("uncontested"), "voters_contested"] = 0
    df = df[(df.result.str.contains("won")) & (~df.election_name.str.contains("By-Election"))]
    df = df[col_idx + col_summary]
    df = pd.concat(
        [df[df.election_name.str.contains("GE-")].assign(state="Malaysia"), df],
        axis=0,
        ignore_index=True,
    )
    df = df.groupby(col_idx).sum().reset_index()
    df = pd.concat(
        [
            df,
            df[
                (~df.state.isin(["Sarawak", "Sabah", "W.P. Labuan", "Malaysia"]))
                & (df.type == "parlimen")
            ]
            .assign(state="Semenanjung")
            .groupby(col_idx)
            .sum()
            .reset_index(),
        ],
        axis=0,
        ignore_index=True,
    )
    df["voter_turnout_perc"] = df.voter_turnout / df.voters_total * 100
    df["votes_rejected_perc"] = df.votes_rejected / df.votes_valid * 100
    df = df.sort_values(by=["type", "state", "election_name"], ascending=[False, True, True])
    df = pd.concat(
        [
            df[df.state == "Malaysia"],
            df[df.state == "Semenanjung"],
            df[~df.state.isin(["Malaysia", "Semenanjung"])],
        ],
        axis=0,
        ignore_index=True,
    )
    print(f"\n{len(df):,.0f} unique permutations")
    write_parquet(f"{PATH_LOCAL_INTERNAL}elections_stats", df=df)


def make_elections_by_seat():
    """
    This function
        - Reads and wrangles the candidate file
        - Filters for only winning candidates
        - Generates a slug for each seat
    Inputs:
        - internal.electiondata.my/candidates.parquet
    Outputs:
        internal.electiondata.my/elections_by_seat.parquet
    """
    # fmt: off
    col_final = [
        "slug", "seat_name", "seat", "state",
        "date", "election_name", "type",
        "party", "party_uid", "coalition", "coalition_uid", "name",
        "majority", "majority_perc", "voter_turnout", "voter_turnout_perc", "votes_rejected", "votes_rejected_perc",
    ]
    # fmt: on

    df = duckdb.query(
        f"SELECT * FROM read_parquet('{PATH_LOCAL_INTERNAL}candidates.parquet') WHERE result LIKE 'won%'"
    ).df()
    df["seat_name"] = df.seat.str[6:]
    df.loc[df.type == "dun", "seat_name"] = df.seat.str[5:]
    df["slug"] = df.seat.apply(generate_slug)
    df = df[col_final]
    print(f"\n{len(df):,.0f} unique contests")
    write_parquet(f"{PATH_LOCAL_INTERNAL}elections_by_seat", df=df)


def make_elections_jsons():
    """Generate election data files for API."""
    col_combo = ["state", "type", "election_name"]
    col_final = {
        "summary": [
            "party_uid",
            "party",
            "coalition",
            "coalition_uid",
            "seats_contested",
            "seats_won",
            "seats_total",
            "seats_contested_perc",
            "seats_won_perc",
            "votes",
            "votes_total",
            "votes_perc",
        ],
        "stats": [
            "voters_total",
            "voter_turnout",
            "voter_turnout_perc",
            "votes_rejected",
            "votes_rejected_perc",
            "n_candidates",
        ],
        "by_seat": [
            "seat",
            "date",
            "party",
            "party_uid",
            "party_lost",
            "name",
            "n_candidates",
            "state",
            "majority",
            "majority_perc",
            "voter_turnout",
            "voter_turnout_perc",
            "votes_rejected",
            "votes_rejected_perc",
        ],
    }

    # dfm for main summary by coalition and party
    dfm = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}parties.parquet").sort_values(
        by=["seats_won_perc", "votes_perc"], ascending=False
    )
    dfm.coalition_uid = dfm.coalition_uid.astype(str).str.zfill(2) + "-" + dfm.coalition

    # dfs for aggregate stats (voter turnout, votes rejected, n_candidates)
    dfs = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}elections_stats.parquet").fillna(0)

    # dft for table of statistics by seat; need to be joined with lf loser frame
    dft = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}elections_by_seat.parquet")
    dft = dft[dft.election_name != "By-Election"]
    dft = pd.concat(
        [
            dft[dft.type == "parlimen"].assign(state="Malaysia"),
            dft[
                (dft.type == "parlimen")
                & (~dft.state.isin(["Sarawak", "Sabah", "W.P. Labuan", "Malaysia"]))
            ].assign(state="Semenanjung"),
            dft,
        ],
        axis=0,
        ignore_index=True,
    )
    dft.date = pd.to_datetime(dft.date).dt.date
    lf = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}consol_ballots.parquet")
    lf = lf[lf.result != "won"]
    lf.loc[lf.result == "won_uncontested", "party"] = "NEMO"
    lf.seat = lf.seat + ", " + lf.state
    lf = lf[["date", "seat", "party"]].drop_duplicates().rename(columns={"party": "party_lost"})
    lf = lf.groupby(["date", "seat"])["party_lost"].agg(list).reset_index()
    dft = pd.merge(dft, lf, on=["date", "seat"], how="left")
    dft["n_candidates"] = dft["party_lost"].apply(lambda x: len(x) + 1)
    dft.loc[dft.voter_turnout == 0, "n_candidates"] = 1

    assert (
        len(dfm.drop_duplicates(subset=col_combo))
        == len(dfs.drop_duplicates(subset=col_combo))
        == len(dft.drop_duplicates(subset=col_combo))
    ), f"Mismatch between 3 components!\
            summaries: {len(dfm.drop_duplicates(subset=col_combo))} \
            stats: {len(dfs.drop_duplicates(subset=col_combo))} \
            by_seat: {len(dft.drop_duplicates(subset=col_combo))}"

    dft.date = pd.to_datetime(dft.date).dt.date.astype(str)
    dfm.date = pd.to_datetime(dfm.date).dt.date.astype(str)
    df = {"summary": dfm, "stats": dfs, "by_seat": dft}

    for election_type in dfm.type.unique():
        tf = dfm[dfm.type == election_type].copy()
        for state in tf.state.unique():
            tf = dfm[(dfm.type == election_type) & (dfm.state == state)].copy().copy()
            for election in tf.election_name.unique():

                # ensure state folder exists
                if not os.path.exists(f"{PATH_LOCAL_INTERNAL}elections/{state}"):
                    os.makedirs(f"{PATH_LOCAL_INTERNAL}elections/{state}")

                # now loop over the keys
                data = {"summary": [], "stats": [], "by_seat": []}
                for key, value in df.items():
                    tf = value.copy()
                    tf = tf[
                        (tf.type == election_type)
                        & (tf.state == state)
                        & (tf.election_name == election)
                    ]
                    res = tf[col_final[key]].to_dict(orient="records")
                    res = [
                        {
                            k: ((None if pd.isna(v) else v) if not isinstance(v, list) else v)
                            for k, v in record.items()
                        }
                        for record in res
                    ]  # proper JSON null
                    res = [
                        {
                            k: [] if isinstance(v, list) and v == ["NEMO"] else v
                            for k, v in record.items()
                        }
                        for record in res
                    ]
                    data[key] = res
                with open(
                    f"{PATH_LOCAL_INTERNAL}elections/{state}/{election_type}-{election}.json",
                    "w",
                    encoding="utf-8",
                ) as f:
                    j.dump(data, f)


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET = os.getenv("R2_BUCKET_INTERNAL")

    make_election_stats()
    make_elections_by_seat()
    make_elections_jsons()

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
