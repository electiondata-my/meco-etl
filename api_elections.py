"""
Module: api_elections.py

This module processes and uploads election summary data for internal.electiondata.my
and the public API at api.electiondata.my.
It:
- Computes per-election voter turnout and rejected vote statistics grouped by state and type
- Builds a per-seat winners table across all general elections and by-elections
- Generates per-election JSON files (aggregate party stats and seat-level breakdowns)
- Generates a consolidated by-elections JSON file
- Uploads all.json to the internal R2 bucket
- Uploads per-state/election JSONs to the API R2 bucket
- Generates a public API byelections JSON with seat field trimmed (no state suffix)
- Uploads the API byelections JSON to the API R2 bucket
- Purges the Cloudflare cache for the elections prefix

Inputs:
- internal.electiondata.my/candidates.parquet (created by api_candidates.py)
- internal.electiondata.my/parties.parquet (created by api_parties.py)
- PATH_RESULTS_HEADLINE/consol_ballots.parquet

Outputs:
- internal.electiondata.my/elections_stats.parquet
- internal.electiondata.my/elections_by_seat.parquet
- internal.electiondata.my/elections/all.json (uploaded to internal R2)
- internal.electiondata.my/elections/{state}/{election}.json (by_seat + stats; uploaded to internal R2)
- api.electiondata.my/v1/elections/{state}/{type}-{election}-aggregate.json (uploaded to API R2)
- api.electiondata.my/v1/elections/{state}/{type}-{election}-by_seat.json (uploaded to API R2)
- api.electiondata.my/v1/elections/byelections.json (uploaded to API R2)
- api.electiondata.my/v1/elections/dropdown.json (mirrored from internal/elections/dropdown.json to API R2)
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
PATH_LOCAL_API = "api.electiondata.my/v1/elections/"


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
        "ballots_not_returned", "ballots_not_returned_perc",
        "votes_valid", "votes_rejected", "votes_rejected_perc",
    ]
    # fmt: on

    df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}candidates.parquet")
    # voter_turnout is a count here (ballots issued); voter_turnout_perc holds the percentage
    df["votes_valid"] = df.voter_turnout - df.ballots_not_returned - df.votes_rejected
    df["voters_contested"] = df["voters_total"]
    df.loc[df.result.str.contains("uncontested"), "voters_contested"] = 0
    df = df[~df.election_name.str.contains("By-Election")]
    # won rows supply full stats; pending rows (one per seat) contribute n_candidates and
    # voters_total while voter_turnout stays 0 — correct for pre-election and election night
    df_pending = df[df.result == "pending"].drop_duplicates(
        subset=["election_name", "state", "seat"]
    )
    df = pd.concat([df[df.result.str.contains("won")], df_pending], ignore_index=True)
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
    # denominator is ballots issued, i.e. voter_turnout as a count
    df["ballots_not_returned_perc"] = df.ballots_not_returned / df.voter_turnout * 100
    # denominator is ballots returned, which votes_valid + votes_rejected reconstructs
    df["votes_rejected_perc"] = df.votes_rejected / (df.votes_valid + df.votes_rejected) * 100
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
        "voters_total", "voter_turnout", "voter_turnout_perc",
        "ballots_not_returned", "ballots_not_returned_perc",
        "majority", "majority_perc", "votes_valid", "votes_rejected", "votes_rejected_perc",
    ]
    # fmt: on

    df = duckdb.query(
        f"SELECT *, voter_turnout - ballots_not_returned - votes_rejected AS votes_valid "
        f"FROM read_parquet('{PATH_LOCAL_INTERNAL}candidates.parquet') WHERE result LIKE 'won%'"
    ).df()
    df["seat_name"] = df.seat.str[6:]
    df.loc[df.type == "dun", "seat_name"] = df.seat.str[5:]
    df["slug"] = df.seat.apply(generate_slug)
    df = df[col_final]
    # add pending elections: one row per seat with null winner fields
    pf = duckdb.query(
        f"SELECT DISTINCT election_name, state, seat, date, type, voters_total FROM read_parquet('{PATH_LOCAL_INTERNAL}candidates.parquet') WHERE result = 'pending'"
    ).df()
    if not pf.empty:
        pf["seat_name"] = pf.seat.str[6:]
        pf.loc[pf.type == "dun", "seat_name"] = pf.seat.str[5:]
        pf["slug"] = pf.seat.apply(generate_slug)
        for col in [c for c in col_final if c not in pf.columns]:
            pf[col] = None
        df = pd.concat([df, pf[col_final]], ignore_index=True)
    print(f"\n{len(df):,.0f} unique contests")
    write_parquet(f"{PATH_LOCAL_INTERNAL}elections_by_seat", df=df)


def make_elections_jsons():
    """Generate election data files for API."""
    col_combo = ["state", "type", "election_name"]
    col_final = {
        "by_party": [
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
            "ballots_not_returned",
            "ballots_not_returned_perc",
            "votes_valid",
            "votes_rejected",
            "votes_rejected_perc",
            "n_candidates",
        ],
        "by_seat": [
            "seat",
            "state",
            "date",
            "name",
            "party",
            "party_uid",
            "coalition",
            "coalition_uid",
            "party_lost",
            "party_lost_uid",
            "coalition_lost",
            "coalition_lost_uid",
            "n_candidates",
            "voters_total",
            "voter_turnout",
            "voter_turnout_perc",
            "ballots_not_returned",
            "ballots_not_returned_perc",
            "majority",
            "majority_perc",
            "votes_valid",
            "votes_rejected",
            "votes_rejected_perc",
        ],
    }

    # dfm for main summary by coalition and party
    dfm = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}parties.parquet").sort_values(
        by=["seats_won_perc", "votes_perc"], ascending=False
    )
    # Use historical names/uids (at time of election) rather than normalised current values
    dfm["party_uid"] = dfm["known_as_uid"]
    dfm["party"] = dfm["known_as"]
    dfm["coalition_uid"] = dfm["known_as_coalition_uid"]
    dfm["coalition"] = dfm["known_as_coalition"]

    # dfs for aggregate stats (total voters, voter turnout, votes rejected, n_candidates)
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
    for c, ph in zip(
        ["party", "party_uid", "coalition", "coalition_uid"], ["NEMO", "NEMO", "NEMO", "NEMO"]
    ):
        lf.loc[lf.result == "won_uncontested", c] = ph
    lf.seat = lf.seat + ", " + lf.state
    lf = lf[["date", "seat", "party", "party_uid", "coalition", "coalition_uid"]].rename(
        columns={
            "party": "party_lost",
            "party_uid": "party_lost_uid",
            "coalition": "coalition_lost",
            "coalition_uid": "coalition_lost_uid",
        }
    )
    lf = (
        lf.groupby(["date", "seat"])[
            ["party_lost", "party_lost_uid", "coalition_lost", "coalition_lost_uid"]
        ]
        .agg(list)
        .reset_index()
    )
    dft = pd.merge(dft, lf, on=["date", "seat"], how="left")
    dft["n_candidates"] = dft["party_lost"].apply(
        lambda x: len(x) + 1 if isinstance(x, list) else 0
    )
    for c in ["party", "coalition"]:
        dft[c + "_lost"] = dft[c + "_lost"].apply(
            lambda x: list(dict.fromkeys(x)) if isinstance(x, list) else []
        )
        dft[c + "_lost_uid"] = dft[c + "_lost_uid"].apply(
            lambda x: list(dict.fromkeys(x)) if isinstance(x, list) else []
        )
    dft.loc[dft.voter_turnout == 0, "n_candidates"] = 1
    # pending seats have no lf match so n_candidates=0 above; fill from the full candidate list
    _pending_n = duckdb.query(f"""
        SELECT election_name, state, seat, COUNT(*) AS n_pending
        FROM read_parquet('{PATH_LOCAL_INTERNAL}candidates.parquet')
        WHERE result = 'pending'
        GROUP BY election_name, state, seat
        """).df()
    dft = pd.merge(dft, _pending_n, on=["election_name", "state", "seat"], how="left")
    dft.loc[dft.n_pending.notna(), "n_candidates"] = dft.loc[
        dft.n_pending.notna(), "n_pending"
    ].astype(int)
    dft = dft.drop(columns=["n_pending"]).sort_values(by="seat").reset_index(drop=True)

    # pending elections appear in dfm (candidates announced) but not in dfs/dft (no winners yet)
    pending_elections = set(dfm.election_name.unique()) - set(dfs.election_name.unique())
    dfm_completed = dfm[~dfm.election_name.isin(pending_elections)]
    dft_completed = dft[~dft.election_name.isin(pending_elections)]
    assert (
        len(dfm_completed.drop_duplicates(subset=col_combo))
        == len(dfs.drop_duplicates(subset=col_combo))
        == len(dft_completed.drop_duplicates(subset=col_combo))
    ), f"Mismatch between 3 components!\
            by_party: {len(dfm_completed.drop_duplicates(subset=col_combo))} \
            stats: {len(dfs.drop_duplicates(subset=col_combo))} \
            by_seat: {len(dft_completed.drop_duplicates(subset=col_combo))}"

    dft.date = pd.to_datetime(dft.date).dt.date.astype(str)
    dfm.date = pd.to_datetime(dfm.date).dt.date.astype(str)
    df = {"by_party": dfm, "stats": dfs, "by_seat": dft}

    all_data = {}

    for election_type in dfm.type.unique():
        tf = dfm[dfm.type == election_type].copy()
        for state in tf.state.unique():
            tf = dfm[(dfm.type == election_type) & (dfm.state == state)].copy().copy()
            for election in tf.election_name.unique():

                # ensure state folder exists
                os.makedirs(f"{PATH_LOCAL_API}{state}", exist_ok=True)

                # now loop over the keys
                data = {"by_party": [], "stats": [], "by_seat": []}
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

                base = f"{PATH_LOCAL_API}{state}/{election}"
                for c in ["by_party", "by_seat", "stats"]:
                    records = data[c]
                    if c == "by_seat":
                        records = []
                        for r in data[c]:
                            r = dict(r)
                            if r.get("seat") and ", " in r["seat"]:
                                seat_part, state_part = r["seat"].rsplit(", ", 1)
                                r["seat"] = seat_part
                                r["state"] = state_part
                            records.append(r)
                    with open(f"{base}-{c}.json", "w", encoding="utf-8") as f:
                        j.dump({c: records}, f)

                os.makedirs(f"{PATH_LOCAL_INTERNAL}elections/{state}", exist_ok=True)
                with open(
                    f"{PATH_LOCAL_INTERNAL}elections/{state}/{election}.json", "w", encoding="utf-8"
                ) as f:
                    j.dump({"by_party": data["by_party"], "stats": data["stats"]}, f)

                all_data.setdefault(state, {}).setdefault(election_type, {})[election] = data

    with open(f"{PATH_LOCAL_INTERNAL}elections/all.json", "w", encoding="utf-8") as f:
        j.dump(all_data, f, sort_keys=True)


def make_byelections_json():
    """
    Builds a flat JSON file listing all by-election results.
    It:
        - Reads elections_by_seat.parquet and filters for By-Election rows only
        - Normalises dates to ISO strings and converts NaN values to JSON null
    Inputs:
        internal.electiondata.my/elections_by_seat.parquet
    Outputs:
        internal.electiondata.my/elections/byelections.json
    """
    col_prk = [
        "seat",
        "state",
        "date",
        "name",
        "party_uid",
        "party",
        "coalition_uid",
        "coalition",
        "voters_total",
        "voter_turnout",
        "voter_turnout_perc",
        "ballots_not_returned",
        "ballots_not_returned_perc",
        "votes_valid",
        "votes_rejected",
        "votes_rejected_perc",
        "majority",
        "majority_perc",
    ]

    df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}elections_by_seat.parquet")
    df = df[df.election_name == "By-Election"][col_prk]
    df.date = pd.to_datetime(df.date).astype(str)

    res = df.to_dict(orient="records")
    res = [
        {
            k: ((None if pd.isna(v) else v) if not isinstance(v, list) else v)
            for k, v in record.items()
        }
        for record in res
    ]  # proper JSON null
    data = {"data": []}
    data["data"] = res

    with open(f"{PATH_LOCAL_INTERNAL}elections/byelections.json", "w", encoding="utf-8") as f:
        j.dump(data, f)


def make_byelections_api_json():
    """Generate the public API byelections JSON.

    Inputs:
    - internal.electiondata.my/elections/byelections.json

    Outputs:
    - api.electiondata.my/v1/elections/byelections.json
    """
    os.makedirs(PATH_LOCAL_API, exist_ok=True)

    with open(f"{PATH_LOCAL_INTERNAL}elections/byelections.json", encoding="utf-8") as f:
        data = j.load(f)

    for record in data["data"]:
        seat_state = record.get("seat")
        if seat_state:
            parts = seat_state.rsplit(", ", 1)
            record["seat"] = parts[0]

    data["data"].sort(key=lambda r: r["date"], reverse=True)

    with open(f"{PATH_LOCAL_API}byelections.json", "w", encoding="utf-8") as f:
        j.dump({"byelections": data["data"]}, f)
    print("Wrote api elections/byelections.json")


def upload_elections_jsons(client, bucket):
    """Upload elections JSON files to R2."""
    files = g(f"{PATH_LOCAL_INTERNAL}elections/*.json") + g(
        f"{PATH_LOCAL_INTERNAL}elections/**/*.json", recursive=True
    )
    print(f"\nUploading {len(files):,.0f} files to R2")
    files_to_upload = sorted([(f, f.replace(PATH_LOCAL_INTERNAL, "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def upload_elections_api_jsons(client, bucket):
    """Upload per-state election JSONs directly to the API R2 bucket."""
    files = g(f"{PATH_LOCAL_API}*/*.json")
    print(f"\nUploading {len(files):,.0f} API election files to R2")
    files_to_upload = sorted([(f, f.replace("api.electiondata.my/", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def upload_byelections_api_json(client, bucket):
    """Upload API byelections JSON to R2."""
    files = g(f"{PATH_LOCAL_API}*.json")
    files_to_upload = sorted([(f, f.replace("api.electiondata.my/", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def sync_dropdown_json_to_api(client, bucket):
    """Mirror elections/dropdown.json from the internal folder to the API R2 bucket."""
    with open(f"{PATH_LOCAL_INTERNAL}elections/dropdown.json", encoding="utf-8") as f:
        data = j.load(f)
    with open(f"{PATH_LOCAL_API}dropdown.json", "w", encoding="utf-8") as f:
        j.dump({"elections": data["elections"]}, f)
    upload_bulk(client, bucket, [(f"{PATH_LOCAL_API}dropdown.json", "v1/elections/dropdown.json")])


def purge_elections_cache(prefix="elections/"):
    """Purge Cloudflare cache for elections JSON files by URL prefix."""
    full_prefix = f"{PATH_LOCAL_INTERNAL}{prefix}"
    print(f"\nPurging cache prefix: {full_prefix}")
    purge_cf_cache_prefix([full_prefix])


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET_INTERNAL = os.getenv("R2_BUCKET_INTERNAL")
    BUCKET_API = os.getenv("R2_BUCKET_API")

    make_election_stats()
    make_elections_by_seat()
    make_elections_jsons()
    make_byelections_json()
    make_byelections_api_json()

    upload_elections_jsons(CLIENT, BUCKET_INTERNAL)
    purge_elections_cache()

    upload_elections_api_jsons(CLIENT, BUCKET_API)
    upload_byelections_api_json(CLIENT, BUCKET_API)
    sync_dropdown_json_to_api(CLIENT, BUCKET_API)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
