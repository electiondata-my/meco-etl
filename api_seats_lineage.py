"""
Module: api_seats_lineage.py

This module generates seat lineage tables for parliamentary (parlimen) and state (DUN) seats.
It produces three types of lineage files:
- geo: maps each seat to its dominant ancestor across delimitation years, with geospatial context
- filter: maps each current seat to past seat names by election, used for extracting historical results
- desc: human-readable descriptions of boundary changes (in English and Malay), keyed by delimitation date

Inputs:
- PATH_MAPS_DELIMS/*_dun.parquet (delimitation boundary files for DUN seats)
- PATH_MAPS_DELIMS/*parlimen*.parquet (delimitation boundary files for parlimen seats, incl. 1954)
- DELIMS_TO_ELECTIONS (CSV mapping delimitation years to elections, by region)
- PATH_LINEAGE/lineage_dun.csv
- PATH_LINEAGE/lineage_parlimen.csv

Outputs (main):
- internal.electiondata.my/lineage/dun_geo.parquet/.csv
- internal.electiondata.my/lineage/parlimen_geo.parquet/.csv
- internal.electiondata.my/lineage/filter.parquet/.csv (consolidated)
- internal.electiondata.my/lineage/desc.parquet/.csv (consolidated)

Outputs (auxiliary):
- internal.electiondata.my/lineage/dun_filter.parquet/.csv
- internal.electiondata.my/lineage/parlimen_filter.parquet/.csv
- internal.electiondata.my/lineage/dun_desc.parquet/.csv
- internal.electiondata.my/lineage/parlimen_desc.parquet/.csv
"""

import os
from datetime import datetime
from glob import glob as g
import duckdb
import pandas as pd
from dotenv import load_dotenv

from helper import write_csv_parquet

load_dotenv()
DELIMS_TO_ELECTIONS = os.getenv("DELIMS_TO_ELECTIONS")
PATH_MAPS_DELIMS = os.getenv("PATH_MAPS_DELIMS")
PATH_LINEAGE = os.getenv("PATH_LINEAGE")
PATH_LOCAL_INTERNAL = "internal.electiondata.my/"


def make_lineage_geo():
    """
    The function:
        Builds geospatial lineage tables for both DUN and parlimen seats across delimitation years.
        For DUN: reads all non-Sarawak-2026 boundary parquets, extracts the delimitation year from
        each filename, then merges against lineage_dun.csv (which maps each current seat to its
        dominant ancestor per delimitation year) to produce a per-seat, per-year lineage table.
        For parlimen: does the same, then additionally injects the 1954 pre-delimitation baseline
        (0 DUNs, null DUN list) and merges against lineage_parlimen.csv. W.P. seats (which have no
        DUNs) are forced to n_duns=0.
    Inputs:
        PATH_MAPS_DELIMS/*_dun.parquet
        PATH_MAPS_DELIMS/*parlimen*.parquet (peninsular_1954 subset)
        PATH_LINEAGE/lineage_dun.csv
        PATH_LINEAGE/lineage_parlimen.csv
    Outputs:
        internal.electiondata.my/lineage/dun_geo.parquet/.csv
        internal.electiondata.my/lineage/parlimen_geo.parquet/.csv
    """
    delims_dun = [f for f in g(f"{PATH_MAPS_DELIMS}*_dun.parquet") if "sarawak_2026" not in f]
    delims_par_1954 = [f for f in g(f"{PATH_MAPS_DELIMS}*.parquet") if "peninsular_1954" in f]

    df = duckdb.sql(
        r"""
        SELECT
            state,
            dun,
            parlimen,
            CAST(regexp_extract(filename, '(\d{4})', 1) AS INTEGER) AS year
        FROM read_parquet($delims, filename=True)
        ORDER BY state ASC, year DESC, dun ASC
    """,
        params={"delims": delims_dun},
    ).df()

    rf = pd.read_csv(
        f"{PATH_LINEAGE}lineage_dun.csv",
        usecols=["current_seat", "state", "date_old", "dominant_ancestor"],
    )
    rf = rf.rename(columns={"date_old": "year", "dominant_ancestor": "dun"})
    rf = pd.merge(rf, df, on=["state", "year", "dun"], how="left")
    write_csv_parquet(f"{PATH_LOCAL_INTERNAL}lineage/dun_geo", rf)

    df = (
        df.groupby(["state", "year", "parlimen"])
        .agg(n_duns=("dun", "count"), duns=("dun", lambda x: ", ".join(sorted(x))))
        .reset_index()
    )
    tf = duckdb.sql(
        """
        SELECT
            state,
            parlimen,
            1954 AS year,
            0 AS n_duns,
            NULL AS duns
        FROM read_parquet($delims, filename=True)
    """,
        params={"delims": delims_par_1954},
    ).df()
    df = pd.concat([df, tf], axis=0, ignore_index=True).sort_values(
        by=["state", "year", "parlimen"], ascending=[True, False, True]
    )

    rf = pd.read_csv(
        f"{PATH_LINEAGE}lineage_parlimen.csv",
        usecols=["current_seat", "state", "date_old", "dominant_ancestor"],
    )
    rf = rf.rename(columns={"date_old": "year", "dominant_ancestor": "parlimen"})
    rf = pd.merge(rf, df, on=["state", "year", "parlimen"], how="left")
    rf.loc[rf.state.str.contains("W.P."), "n_duns"] = 0
    rf.n_duns = rf.n_duns.astype(int)
    write_csv_parquet(f"{PATH_LOCAL_INTERNAL}lineage/parlimen_geo", rf)


def make_lineage_filter_desc(seat_type="parlimen"):
    """
    The function:
        Generates two lineage artefacts for the given seat type (parlimen or dun):

        1. Filter file: maps each current seat to the ancestor seat name that was active at the
           time of each election. Built by joining DELIMS_TO_ELECTIONS (which maps each election
           to the delimitation vintage it used, separately for Peninsular/Sabah/Sarawak) against
           the lineage CSV on (state_join, date_old). After building descriptions (step 2), the
           filter is finalised by appending the state name to each current_seat value.

        2. Desc file: produces human-readable boundary change descriptions (English + Malay) for
           each seat, keyed by a display date. The display date is -12-31 when the delimitation
           year coincides with an election year (to distinguish the new boundary from the old one
           used in that same election), and -01-01 otherwise.

        The function also validates the final filter — every current_seat must be present in the
        name map derived from the desc step, and raises an AssertionError if any are missing.
    Args:
        seat_type (str): "parlimen" or "dun". Determines which lineage CSV and output paths to use.
    Inputs:
        DELIMS_TO_ELECTIONS
        PATH_LINEAGE/lineage_{seat_type}.csv
        internal.electiondata.my/lineage/{seat_type}_filter.parquet (re-read for final validation)
    Outputs:
        internal.electiondata.my/lineage/{seat_type}_filter.parquet/.csv
        internal.electiondata.my/lineage/{seat_type}_desc.parquet/.csv
    """

    # ----- generate initial filter file -----

    df = pd.read_csv(DELIMS_TO_ELECTIONS, dtype="str")
    if seat_type == "parlimen":
        df = df[df.state == "Malaysia"].drop(columns=["state", "year"])
    else:
        df = df[df.state != "Malaysia"].drop(columns=["year"])
    df = df.rename(columns={"peninsular": "Peninsular", "sabah": "Sabah", "sarawak": "Sarawak"})
    df = df.melt(
        id_vars=["election"] if seat_type == "parlimen" else ["election", "state"],
        value_vars=["Peninsular", "Sabah", "Sarawak"],
        var_name="state_join",
        value_name="date_old",
    )
    df = df[df.date_old != "0"]

    res = pd.read_csv(
        f"{PATH_LINEAGE}lineage_{seat_type}.csv",
        dtype="str",
        usecols=["current_seat", "state", "date_old", "dominant_ancestor"],
    )
    res["state_join"] = res.state
    res.loc[~res.state.isin(["Sabah", "Sarawak"]), "state_join"] = "Peninsular"

    # A state election maps only to its own state's seats: SE-N is a per-state label, and two
    # states sharing it need not share a delimitation vintage. Parliamentary elections are
    # national, so they still match every seat in the region.
    if seat_type == "parlimen":
        df = pd.merge(df, res, on=["state_join", "date_old"], how="left").drop_duplicates()
    else:
        df = pd.merge(
            df.drop(columns=["state_join"]), res, on=["state", "date_old"], how="left"
        ).drop_duplicates()
    df = df[["current_seat", "election", "state", "dominant_ancestor"]].rename(
        columns={"dominant_ancestor": "seat"}
    )
    df = df.sort_values(by=["state", "current_seat", "election"])
    write_csv_parquet(f"{PATH_LOCAL_INTERNAL}lineage/{seat_type}_filter", df)

    # ----- generate lineage descriptions -----

    def assign_date(delim_year, prev_delim_year, election_year, election_years):
        """
        Returns a display date string for a delimitation entry.
        Uses -12-31 when the delimitation year is itself an election year and differs from the
        previous delimitation year, signalling that the new boundary took effect after that
        election. Returns -01-01 in all other cases.
        """
        if (
            delim_year in election_years
            and delim_year != election_year
            and prev_delim_year != delim_year
        ):
            return f"{delim_year}-12-31"
        return f"{delim_year}-01-01"

    df = pd.read_csv(DELIMS_TO_ELECTIONS, dtype="str")
    if seat_type == "parlimen":
        df = df[df.state == "Malaysia"].drop(columns=["election"])
    else:
        df = df[df.state != "Malaysia"].drop(columns=["election"])
    df = df.rename(columns={"peninsular": "Peninsular", "sabah": "Sabah", "sarawak": "Sarawak"})
    df = df.melt(
        id_vars=["state", "year"],
        value_vars=["Peninsular", "Sabah", "Sarawak"],
        var_name="state_join",
        value_name="date",
    )
    df = df[df.date != "0"]
    df["date_prev"] = df.groupby("state")["date"].shift(1).fillna("")
    if seat_type == "parlimen":
        election_years = list(df["year"].unique())
        df["election_years"] = [election_years] * len(df)
    else:
        df["election_years"] = df["state"].map(
            lambda s: list(df[df["state"] == s]["year"].unique())
        )
    df["date_disp"] = df.apply(
        lambda x: assign_date(x["date"], x["date_prev"], x["year"], x["election_years"]), axis=1
    )
    df = df.drop(columns=["date_prev", "year", "election_years"]).rename(
        columns={"date": "date_new"}
    )

    res = pd.read_csv(
        f"{PATH_LINEAGE}lineage_{seat_type}.csv",
        dtype="str",
        usecols=["state", "current_seat", "date_new", "change_en", "change_ms"],
    ).dropna(how="any")
    res["state_join"] = res.state
    res.loc[~res.state.isin(["Sabah", "Sarawak"]), "state_join"] = "Peninsular"
    res = pd.merge(res, df.drop("state", axis=1), on=["state_join", "date_new"], how="left")
    res = (
        res[["state", "current_seat", "date_disp", "change_en", "change_ms"]]
        .rename(columns={"date_disp": "date"})
        .drop_duplicates()
    )

    res = res.sort_values(by=["current_seat"])
    mf = res[["state", "current_seat"]].drop_duplicates(subset=["current_seat"])
    map_current_state = dict(zip(mf.current_seat, mf.state))
    res["state"] = res.current_seat.map(map_current_state)
    res.current_seat = res.current_seat + ", " + res.state
    if seat_type == "parlimen":
        res = (
            res.sort_values(by=["current_seat"])
            .drop(columns=["state"])
            .rename(columns={"current_seat": "seat"})
        )
    else:
        res = (
            res.sort_values(by=["state", "current_seat"])
            .drop(columns=["state"])
            .rename(columns={"current_seat": "seat"})
        )
    write_csv_parquet(f"{PATH_LOCAL_INTERNAL}lineage/{seat_type}_desc", res)

    # ----- generate final validated filter file -----

    df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}lineage/{seat_type}_filter.parquet")
    assert (
        len(df[~df.current_seat.isin(map_current_state.keys())]) == 0
    ), "Missing current seat in map_current_state"
    df["current_seat"] = df.current_seat + ", " + df.current_seat.map(map_current_state)
    write_csv_parquet(f"{PATH_LOCAL_INTERNAL}lineage/{seat_type}_filter", df)


def make_lineage_filter_desc_consolidated():
    """
    The function:
        Concatenates the parlimen and DUN lineage artefacts (filter and desc) into single
        combined files covering all seat types. The election column is renamed to election_name
        for consistency with the rest of the API layer.
    Inputs:
        internal.electiondata.my/lineage/parlimen_filter.parquet
        internal.electiondata.my/lineage/dun_filter.parquet
        internal.electiondata.my/lineage/parlimen_desc.parquet
        internal.electiondata.my/lineage/dun_desc.parquet
    Outputs:
        internal.electiondata.my/lineage/filter.parquet/.csv
        internal.electiondata.my/lineage/desc.parquet/.csv
    """
    for f in ["filter", "desc"]:
        df = pd.concat(
            [
                pd.read_parquet(f"{PATH_LOCAL_INTERNAL}lineage/parlimen_{f}.parquet"),
                pd.read_parquet(f"{PATH_LOCAL_INTERNAL}lineage/dun_{f}.parquet"),
            ]
        ).rename(columns={"election": "election_name"})
        write_csv_parquet(f"{PATH_LOCAL_INTERNAL}lineage/{f}", df)


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')
    make_lineage_geo()
    make_lineage_filter_desc(seat_type="parlimen")
    make_lineage_filter_desc(seat_type="dun")
    make_lineage_filter_desc_consolidated()
    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
