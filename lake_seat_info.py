"""
Module: lake_seat_info.py

Builds per-seat voter demographics from the public voter rolls and uploads them to the data lake.
It:
- Reads every public voter roll in PATH_VR, named {ge}{NN}_{year} or {state}_se{NN}_{year}
- Counts voters per seat by sex, age band, ethnicity and voter type, at Parliament and DUN level
  (state election rolls produce DUN rows only)
- Dates each row from the headline results, so rows join to results on date, election, state, seat
- Writes a national file, plus Sabah and Sarawak files with state-specific ethnic groupings
- Checks that every breakdown sums to voters_total, and that every contested seat matches the results
- Uploads the three parquets to R2 meco-lake, then updates the lake freshness manifest last

Inputs:
- PATH_VR/{ge12_2008,...,mlk_se16_2026}.parquet
- PATH_RESULTS_HEADLINE/consol_stats.parquet

Outputs:
- lake.electiondata.my/seat_info/demographics.parquet uploaded to R2
- lake.electiondata.my/seat_info/demographics_sabah.parquet uploaded to R2
- lake.electiondata.my/seat_info/demographics_sarawak.parquet uploaded to R2
- core-freshness.json updated in R2 meco-lake

Pass --skip-upload to build and check the files locally without uploading.
"""

import os
import re
import sys
from datetime import datetime
from glob import glob as g

import duckdb
import pandas as pd
from dotenv import load_dotenv

from helper import get_r2_client, get_states, upload_bulk, write_lake_manifest

load_dotenv()
PATH_VR = os.getenv("PATH_VR")
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")
PATH_LOCAL_LAKE = "lake.electiondata.my/"
PATH_OUT = f"{PATH_LOCAL_LAKE}seat_info/"

STATE_BY_CODE = dict(zip(get_states(codes=1), get_states(codes=0)))

AGE_BANDS = [
    # open downward: a handful of records in the EC's own rolls fall below the voting age,
    # and every voter must land in exactly one bucket
    ("age_18_20", "<= 20"),
    ("age_21_29", "BETWEEN 21 AND 29"),
    ("age_30_39", "BETWEEN 30 AND 39"),
    ("age_40_49", "BETWEEN 40 AND 49"),
    ("age_50_59", "BETWEEN 50 AND 59"),
    ("age_60_69", "BETWEEN 60 AND 69"),
    ("age_70_79", "BETWEEN 70 AND 79"),
    ("age_80_89", "BETWEEN 80 AND 89"),
    ("age_90+", ">= 90"),
]

BUMI_SABAH = [
    "Bajau",
    "Dusun",
    "Kadazan",
    "Murut",
    "Brunei",
    "Sungai",
    "Other Bumi Sabah",
    "Bumi Sabah",
]
BUMI_SARAWAK = [
    "Iban",
    "Bidayuh",
    "Melanau",
    "Kedayan",
    "Orang Ulu",
    "Other Bumi Sarawak",
    "Bumi Sarawak",
]

# Each grouping maps output column -> roll ethnicity codes. "ethnic_other" is the residual:
# every code not listed elsewhere in the grouping, so the columns always sum to voters_total.
ETHNIC_GROUPINGS = {
    "national": {
        "ethnic_malay": ["Malay"],
        "ethnic_chinese": ["Chinese"],
        "ethnic_indian": ["Indian"],
        "ethnic_bumi_sabah": BUMI_SABAH,
        "ethnic_bumi_sarawak": BUMI_SARAWAK,
        "ethnic_orang_asli": ["Orang Asli"],
        "ethnic_other": None,
    },
    # Indian, cross-state Bumi Sarawak, generic Bumi Sabah etc. fold into ethnic_other
    "sabah": {
        "ethnic_malay": ["Malay"],
        "ethnic_chinese": ["Chinese"],
        "ethnic_kadazan_dusun": ["Kadazan", "Dusun"],
        "ethnic_bajau": ["Bajau"],
        "ethnic_murut": ["Murut"],
        "ethnic_brunei": ["Brunei"],
        "ethnic_sungai": ["Sungai"],
        "ethnic_other_bumi_sabah": ["Other Bumi Sabah"],
        "ethnic_other": None,
    },
    # Indian, cross-state Bumi Sabah, generic Bumi Sarawak etc. fold into ethnic_other
    "sarawak": {
        "ethnic_malay_melanau": ["Malay", "Melanau"],
        "ethnic_chinese": ["Chinese"],
        "ethnic_iban": ["Iban"],
        "ethnic_bidayuh": ["Bidayuh"],
        "ethnic_orang_ulu": ["Orang Ulu"],
        "ethnic_kedayan": ["Kedayan"],
        "ethnic_other_bumi_sarawak": ["Other Bumi Sarawak"],
        "ethnic_other": None,
    },
}

# grouping -> (output file stem, state filter); None means all states
OUTPUTS = {
    "national": ("demographics", None),
    "sabah": ("demographics_sabah", "Sabah"),
    "sarawak": ("demographics_sarawak", "Sarawak"),
}

VOTERTYPE_COLS = ["votertype_regular", "votertype_early", "votertype_postal_overseas"]


def sql_list(values):
    """Render a list of strings as a SQL IN-list."""
    return ", ".join(f"'{v}'" for v in values)


def ethnic_exprs(grouping):
    """Build the COUNT FILTER expressions for one ethnic grouping."""
    mapping = ETHNIC_GROUPINGS[grouping]
    listed = [code for codes in mapping.values() if codes for code in codes]
    exprs = []
    for col, codes in mapping.items():
        cond = f"ethnicity IN ({sql_list(codes)})" if codes else f"ethnicity NOT IN ({sql_list(listed)})"
        exprs.append(f"COUNT(*) FILTER (WHERE {cond}) AS {col}")
    return ",\n        ".join(exprs)


def parse_roll(path):
    """Return (election, state or None, reference year) from a roll filename like ge15_2022 or jhr_se16_2026."""
    stem = os.path.basename(path).removesuffix(".parquet")
    if m := re.fullmatch(r"ge(\d{2})_(\d{4})", stem):
        return f"GE-{m[1]}", None, int(m[2])
    if m := re.fullmatch(r"([a-z]{3})_se(\d{2})_(\d{4})", stem):
        return f"SE-{m[2]}", STATE_BY_CODE[m[1].upper()], int(m[3])
    raise ValueError(f"Unrecognised voter roll filename: {stem}")


def get_seat_dates():
    """Return non-by-election contests from the headline results: election, state, seat, date."""
    sf = pd.read_parquet(
        f"{PATH_RESULTS_HEADLINE}consol_stats.parquet",
        columns=["date", "election", "state", "seat"],
    )
    sf = sf[sf.election != "BY-ELECTION"].copy()
    sf["date"] = pd.to_datetime(sf.date).dt.date
    assert not sf.duplicated(["election", "state", "seat"]).any(), "Duplicate contests in results!"
    return sf


def build_roll(path, grouping, state_filter, seat_dates):
    """Aggregate one roll into per-seat demographic rows for one ethnic grouping."""
    election, roll_state, year = parse_roll(path)
    levels = ["dun"] if election.startswith("SE-") else ["parlimen", "dun"]
    where_state = f"AND state = '{state_filter}'" if state_filter else ""
    age_exprs = ",\n        ".join(
        f'COUNT(*) FILTER (WHERE {year} - birth_year {cond}) AS "{col}"' for col, cond in AGE_BANDS
    )

    frames = []
    for level in levels:
        frames.append(
            duckdb.query(f"""
    SELECT
        '{election}' AS election,
        state,
        {level} AS seat,
        COUNT(*) AS voters_total,
        COUNT(*) FILTER (WHERE sex = 'Male') AS sex_male,
        COUNT(*) FILTER (WHERE sex = 'Female') AS sex_female,
        {age_exprs},
        {ethnic_exprs(grouping)},
        COUNT(*) FILTER (WHERE dm NOT LIKE '%/00 %' AND dm NOT LIKE '%/UP %') AS votertype_regular,
        COUNT(*) FILTER (WHERE dm LIKE '%/00 %') AS votertype_early,
        COUNT(*) FILTER (WHERE dm LIKE '%/UP %') AS votertype_postal_overseas
    FROM read_parquet('{path}')
    WHERE {level} IS NOT NULL {where_state}
    GROUP BY state, {level}
    """).fetchdf()
        )
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        return df
    if roll_state:
        assert set(df.state) == {roll_state}, f"{path}: roll holds states {set(df.state)}"

    # date each seat from its contest in the results; seats with no contest of this election
    # (DUN seats from a GE roll, e.g. Sabah DUNs at GE-15) take the election's main date
    contests = seat_dates[seat_dates.election == election]
    df = df.merge(contests, on=["election", "state", "seat"], how="left")
    main_date = (
        contests[contests.state == roll_state] if roll_state else contests
    ).date.mode()
    assert len(main_date) == 1, f"{path}: no single date for {election} in results"
    df["date"] = df.date.fillna(main_date.iloc[0])

    # every Parliament seat in a GE roll, and every DUN seat in an SE roll, must be a real contest
    is_contest_level = df.seat.str.startswith("P.") == election.startswith("GE-")
    unmatched = df[is_contest_level & ~df.set_index(["election", "state", "seat"]).index.isin(
        contests.set_index(["election", "state", "seat"]).index
    )]
    assert unmatched.empty, f"{path}: seats not in results:\n{unmatched[['state', 'seat']]}"
    return df


def validate(df, label, grouping):
    """Check that each breakdown sums to voters_total for every seat; raise if any does not."""
    groups = {
        "age": [col for col, _ in AGE_BANDS],
        "ethnic": list(ETHNIC_GROUPINGS[grouping]),
        "votertype": VOTERTYPE_COLS,
    }
    for name, cols in groups.items():
        bad = (df[cols].sum(axis=1) != df.voters_total).sum()
        assert not bad, f"[{label}] {bad} seats where {name} totals != voters_total"
    sex_gap = (df.voters_total - df.sex_male - df.sex_female).sum()
    if sex_gap:
        print(f"  NOTE [{label}]: {sex_gap:,.0f} voters with sex not Male/Female")


def make_seat_info():
    """Build all three demographics files; return their local paths."""
    rolls = sorted(g(f"{PATH_VR}*.parquet"))
    seat_dates = get_seat_dates()
    os.makedirs(PATH_OUT, exist_ok=True)

    paths = []
    for grouping, (stem, state_filter) in OUTPUTS.items():
        print(f"\nBuilding {stem} from {len(rolls)} rolls")
        frames = []
        for roll in rolls:
            df = build_roll(roll, grouping, state_filter, seat_dates)
            if df.empty:
                continue
            validate(df, f"{stem}/{os.path.basename(roll)}", grouping)
            frames.append(df)
            print(f"  {os.path.basename(roll)}: {len(df):,.0f} seats")

        out = pd.concat(frames, ignore_index=True)
        out["is_dun"] = ~out.seat.str.startswith("P.")
        out = (
            out.sort_values(["date", "election", "is_dun", "state", "seat"],
                            ascending=[False, True, True, True, True])
            .drop(columns="is_dun")
            .reset_index(drop=True)
        )
        out = out[["date"] + [c for c in out.columns if c != "date"]]
        assert not out.duplicated(["date", "election", "state", "seat"]).any(), f"{stem}: duplicate keys!"

        path = f"{PATH_OUT}{stem}.parquet"
        out.to_parquet(path, index=False, compression="zstd")
        paths.append(path)
        print(f"  Saved {len(out):,.0f} rows -> {path}")
    return paths


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    PATHS = make_seat_info()

    if "--skip-upload" in sys.argv:
        print("\n--skip-upload: not uploading")
    else:
        CLIENT = get_r2_client()
        BUCKET_LAKE = os.getenv("R2_BUCKET_LAKE")
        print(f"\nUploading {len(PATHS)} parquet files to R2")
        UPLOADED = upload_bulk(
            CLIENT,
            BUCKET_LAKE,
            [(p, p.replace(PATH_LOCAL_LAKE, "")) for p in PATHS],
            content_type="application/octet-stream",
        )
        write_lake_manifest(CLIENT, BUCKET_LAKE, UPLOADED)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"Duration: {datetime.now() - START}")
