"""
Module: api_parties.py

Aggregates candidate-level results to party and coalition level, applies lineage normalisation, and generates JSON outputs for internal and public use.
It:
- Reads candidates.parquet and applies party/coalition lineage (numeric prefix → current uid, historical uid preserved in known_as_uid / known_as_coalition_uid)
- Aggregates to party x election x state and coalition x election x state, writing parties.parquet and coalitions.parquet
- Generates parties/dropdown.json — all contested party variants and all coalition variants, each with a maps_to field pointing to the canonical current uid
- Generates parties/all.json — full time-series of results keyed by party-{uid} or coalition-{uid}
- Splits all.json into individual files under api.electiondata.my/v1/parties/ and api.electiondata.my/v1/coalitions/
- Uploads everything to R2 and purges the Cloudflare cache

Inputs:
- internal.electiondata.my/candidates.parquet
- {PATH_RESULTS_HEADLINE}/lookup_party.parquet
- {PATH_RESULTS_HEADLINE}/lookup_coalition.parquet
- {PATH_RESULTS_HEADLINE}/lookup_dates.parquet

Outputs:
- internal.electiondata.my/parties.parquet
- internal.electiondata.my/coalitions.parquet
- internal.electiondata.my/parties/dropdown.json uploaded to internal R2
- internal.electiondata.my/parties/all.json uploaded to internal R2
- api.electiondata.my/v1/parties/dropdown.json uploaded to API R2
- api.electiondata.my/v1/parties/{uid}/{state}-{type}.json uploaded to API R2 (31 files per party)
- api.electiondata.my/v1/coalitions/{uid}/{state}-{type}.json uploaded to API R2 (31 files per coalition)
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
PATH_LOCAL_API_PARTIES = "api.electiondata.my/v1/parties/"
PATH_LOCAL_API_COALITIONS = "api.electiondata.my/v1/coalitions/"

STATES_PARLIMEN = [
    "Malaysia", "Semenanjung",
    "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan",
    "Pahang", "Perak", "Perlis", "Pulau Pinang",
    "Sabah", "Sarawak", "Selangor", "Terengganu",
    "W.P. Kuala Lumpur", "W.P. Labuan", "W.P. Putrajaya",
]
STATES_DUN = [
    "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan",
    "Pahang", "Perak", "Perlis", "Pulau Pinang",
    "Sabah", "Sarawak", "Selangor", "Terengganu",
]
COMBOS = [(s, "parlimen") for s in STATES_PARLIMEN] + [(s, "dun") for s in STATES_DUN]


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
    map_party_uid_party_acronym = dict(zip(pf.party_uid, pf.party))
    pf["uid"] = pf.party_uid.str.split("-").str[0]
    pf = pf[["uid", "party_uid", "party"]].drop_duplicates(subset=["uid"], keep="last")
    map_uid_party_uid = dict(zip(pf.uid, pf.party_uid))

    cf = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}lookup_coalition.parquet")
    map_coalition_uid_coalition = dict(zip(cf.coalition_uid, cf.coalition))
    cf["uid"] = cf.coalition_uid.str.split("-").str[0]
    cf = cf[["uid", "coalition_uid", "coalition"]].drop_duplicates(subset=["uid"], keep="last")
    map_uid_coalition_uid = dict(zip(cf.uid, cf.coalition_uid))

    col_idx = [
        "party_uid",
        "party",
        "known_as_uid",
        "known_as",
        "coalition",
        "coalition_uid",
        "known_as_coalition_uid",
        "known_as_coalition",
        "type",
        "state",
        "election_name",
        "date",
    ]
    df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}candidates.parquet")
    df["known_as_uid"] = df["party_uid"]
    df["known_as"] = df["party_uid"].map(map_party_uid_party_acronym)
    df["party_uid"] = df.party_uid.str.split("-").str[0].map(map_uid_party_uid)
    df["party"] = df.party_uid.map(map_party_uid_party_acronym)
    df["known_as_coalition_uid"] = df["coalition_uid"]
    df["known_as_coalition"] = df["coalition_uid"].map(map_coalition_uid_coalition)
    df["coalition_uid"] = df.coalition_uid.str.split("-").str[0].map(map_uid_coalition_uid)
    df["coalition"] = df.coalition_uid.map(map_coalition_uid_coalition)
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

    df = df.drop(columns=["party_uid", "party", "known_as_uid", "known_as"])
    df = df.groupby(col_idx[4:]).sum().reset_index()
    df.seats_total = ((df.seats_contested * 100) / (df.seats_contested_perc)).round(0).astype(int)
    df.votes_total = ((df.votes * 100) / (df.votes_perc)).round(0).astype(int)
    for c in ["votes_total", "seats_total"]:
        assert len(df.drop_duplicates(subset=["election_name", "state"])) == len(
            df.drop_duplicates(subset=["election_name", "state", c])
        )
    write_parquet(f"{PATH_LOCAL_INTERNAL}coalitions", df)


def make_parties_jsons():
    """Generate party data files for API."""

    # -------- dropdown --------
    data = {"data": []}

    pf = pd.read_parquet(
        f"{PATH_RESULTS_HEADLINE}lookup_party.parquet",
        columns=["party_uid", "party", "party_name_en", "party_name_bm"],
    )
    pf_norm = pf.copy()
    pf_norm["prefix"] = pf_norm.party_uid.str.split("-").str[0]
    pf_norm = pf_norm.drop_duplicates(subset=["prefix"], keep="last")
    map_prefix_party_uid = dict(zip(pf_norm.prefix, pf_norm.party_uid))

    parties_df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}parties.parquet")
    parties_contested = set(parties_df.known_as_uid.unique())

    df = pf[pf.party_uid.isin(parties_contested)].copy()
    df["maps_to"] = df.party_uid.str.split("-").str[0].map(map_prefix_party_uid)
    df = df.rename(
        columns={
            "party_uid": "uid",
            "party": "acronym",
            "party_name_en": "name_en",
            "party_name_bm": "name_bm",
        }
    )

    cf = pd.read_parquet(
        f"{PATH_RESULTS_HEADLINE}lookup_coalition.parquet",
        columns=["coalition_uid", "coalition", "coalition_name_en", "coalition_name_bm"],
    )
    cf = cf[cf.coalition_uid != "000-ALONE"]
    cf_norm = cf.copy()
    cf_norm["prefix"] = cf_norm.coalition_uid.str.split("-").str[0]
    cf_norm = cf_norm.drop_duplicates(subset=["prefix"], keep="last")
    map_prefix_coalition_uid = dict(zip(cf_norm.prefix, cf_norm.coalition_uid))

    tf = cf.copy()
    tf["maps_to"] = tf.coalition_uid.str.split("-").str[0].map(map_prefix_coalition_uid)
    tf = tf.rename(
        columns={
            "coalition_uid": "uid",
            "coalition": "acronym",
            "coalition_name_en": "name_en",
            "coalition_name_bm": "name_bm",
        }
    )

    df = pd.concat(
        [
            df.assign(type="party").sort_values(by="acronym"),
            tf.assign(type="coalition").sort_values(by="acronym"),
        ],
        axis=0,
        ignore_index=True,
    )
    df = df[["type", "uid", "maps_to", "acronym", "name_en", "name_bm"]].to_dict(orient="records")

    data["data"] = df

    with open(f"{PATH_LOCAL_INTERNAL}parties/dropdown.json", "w", encoding="utf-8") as f:
        j.dump(data, f, ensure_ascii=False)

    # -------- all.json --------
    col_party = [
        "state",
        "type",
        "known_as_uid",
        "known_as",
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
    # Use the historical coalition name (at time of election) rather than the normalised current name
    df["coalition_uid"] = df["known_as_coalition_uid"]
    df["coalition"] = df["known_as_coalition"]
    df = df.drop(columns=["known_as_coalition_uid", "known_as_coalition"])

    tf = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}coalitions.parquet").rename(
        columns={
            "coalition_uid": "party_uid",
            "coalition": "party",
            "known_as_coalition_uid": "known_as_uid",
            "known_as_coalition": "known_as",
        }
    )
    tf = tf[tf.party_uid != "000-ALONE"]
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

    df["key"] = df.apply(
        lambda r: ("party-" if r.party_type == "party" else "coalition-") + r.party_uid, axis=1
    )
    all_data = {
        key: group.drop(columns=["party_uid", "key"]).to_dict(orient="records")
        for key, group in df.groupby("key", sort=True)
    }

    with open(f"{PATH_LOCAL_INTERNAL}parties/all.json", "w", encoding="utf-8") as f:
        j.dump(all_data, f)
        print("Wrote parties/all.json")


def make_api_parties_jsons():
    """Split internal all.json into per-state/type files under uid folders for the public API."""
    os.makedirs(PATH_LOCAL_API_PARTIES, exist_ok=True)
    os.makedirs(PATH_LOCAL_API_COALITIONS, exist_ok=True)

    with open(f"{PATH_LOCAL_INTERNAL}parties/dropdown.json", encoding="utf-8") as f:
        dropdown = j.load(f)
    with open(f"{PATH_LOCAL_API_PARTIES}dropdown.json", "w", encoding="utf-8") as f:
        j.dump(dropdown, f, ensure_ascii=False)
    print("Wrote api parties/dropdown.json")

    with open(f"{PATH_LOCAL_INTERNAL}parties/all.json", encoding="utf-8") as f:
        all_data = j.load(f)

    party_count = coalition_count = 0
    for key, records in all_data.items():
        if key.startswith("party-"):
            uid = key[len("party-"):]
            base_path = f"{PATH_LOCAL_API_PARTIES}{uid}/"
            party_count += 1
        elif key.startswith("coalition-"):
            uid = key[len("coalition-"):]
            base_path = f"{PATH_LOCAL_API_COALITIONS}{uid}/"
            coalition_count += 1
        else:
            continue

        os.makedirs(base_path, exist_ok=True)

        # index existing records by (state, election_type)
        is_coalition = key.startswith("coalition-")
        lookup = {}
        for r in records:
            if is_coalition:
                r = {k: v for k, v in r.items() if k not in ("coalition", "coalition_uid")}
            lookup.setdefault((r["state"], r["type"]), []).append(r)

        for state, election_type in COMBOS:
            with open(f"{base_path}{state}-{election_type}.json", "w", encoding="utf-8") as f:
                j.dump({"results": lookup.get((state, election_type), [])}, f, ensure_ascii=False)

    print(f"Wrote {party_count} party folders and {coalition_count} coalition folders (31 files each)")


def upload_parties_jsons(client, bucket, file_pattern="parties/*"):
    """Upload internal data files matching pattern to R2."""
    files = g(f"{PATH_LOCAL_INTERNAL}{file_pattern}.json")
    print(f"\nUploading {len(files):,.0f} files to internal R2")
    files_to_upload = sorted([(f, f.replace(PATH_LOCAL_INTERNAL, "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def upload_api_parties_jsons(client, bucket):
    """Upload party and coalition API JSON files to the API R2 bucket."""
    files = (
        g(f"{PATH_LOCAL_API_PARTIES}*.json")
        + g(f"{PATH_LOCAL_API_PARTIES}*/*.json")
        + g(f"{PATH_LOCAL_API_COALITIONS}*/*.json")
    )
    print(f"\nUploading {len(files):,.0f} API party/coalition files to R2")
    files_to_upload = sorted([(f, f.replace("api.electiondata.my/", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def purge_parties_cache(prefix="parties/"):
    """Purge Cloudflare cache for party JSON files by URL prefix."""
    full_prefix = f"{PATH_LOCAL_INTERNAL}{prefix}"
    print(f"\nPurging cache prefix: {full_prefix}")
    purge_cf_cache_prefix([full_prefix])


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET_INTERNAL = os.getenv("R2_BUCKET_INTERNAL")
    BUCKET_API = os.getenv("R2_BUCKET_API")

    make_parties_df()
    make_parties_jsons()
    upload_parties_jsons(CLIENT, BUCKET_INTERNAL)
    purge_parties_cache()

    make_api_parties_jsons()
    upload_api_parties_jsons(CLIENT, BUCKET_API)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
