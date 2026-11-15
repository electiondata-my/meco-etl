"""
Module: lake_maps_lookup.py

Builds the delimitation lookup (which boundary set every contest was fought on) and uploads it to the data lake.
It:
- Reads the curated crosswalk DELIMS_TO_ELECTIONS, which gives the delimitation year each general and
  state election used, separately for Peninsular, Sabah and Sarawak
- Dates each one from the headline results (per state, since Sabah and Sarawak sometimes polled later)
- Collapses consecutive elections on the same delimitation into one validity range per state and level,
  so by-elections (and any future by-election) resolve by date without rerunning this script
- Checks every contest, by-elections included, against the maps: the seat name (without its P./N. code,
  since Sabah and Sarawak Parliament seats are renumbered when the Peninsula gains seats) must exist in
  the delimitation its date resolves to
- Reports how many polling districts (DMs) in the saluran results have a shape in the matching subdivision
  map, and fails if any are missing where that map exists
- Uploads the lookup to R2 meco-lake, then updates the lake freshness manifest last; the MCP server
  joins results to map shapes through it

Part of the new-election workflow (README, "Nomination day"): once the election's pending rows are in
consol_stats.parquet, add it to DELIMS_TO_ELECTIONS in paper-meco-maps and rerun this script. The script
refuses to build while any general or state election in the results is missing from the crosswalk.

Inputs:
- DELIMS_TO_ELECTIONS (paper-meco-maps/data/delims_to_elections.csv)
- PATH_RESULTS_HEADLINE/consol_stats.parquet
- PATH_MAPS_DELIMS/{region}_{year}_{parlimen,dun}.parquet
- PATH_MAPS_SUBDIVISIONS/{region}_{year}_dm.parquet
- lake.electiondata.my/results_saluran/*_stats.parquet

Outputs:
- lake.electiondata.my/maps/lookup_delimitation.parquet uploaded to R2
- core-freshness.json updated in R2 meco-lake

Pass --skip-upload to build and check the file locally without uploading.
"""

import os
import sys
from datetime import datetime

import duckdb
import pandas as pd
from dotenv import load_dotenv

from helper import get_r2_client, upload_bulk, write_lake_manifest

load_dotenv()
DELIMS_TO_ELECTIONS = os.getenv("DELIMS_TO_ELECTIONS")
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")
PATH_MAPS_DELIMS = os.getenv("PATH_MAPS_DELIMS")
PATH_MAPS_SUBDIVISIONS = os.getenv("PATH_MAPS_SUBDIVISIONS")
PATH_LOCAL_LAKE = "lake.electiondata.my/"
PATH_OUT = f"{PATH_LOCAL_LAKE}maps/lookup_delimitation.parquet"

REGION_SQL = "CASE state WHEN 'Sabah' THEN 'sabah' WHEN 'Sarawak' THEN 'sarawak' ELSE 'peninsular' END"
STRIP_CODE = r"regexp_replace({}, '^\S+\s+', '')"  # 'P.166 Mas Gading' -> 'Mas Gading'


def read_maps(con):
    """Register the map attributes (no geometry) as tables delims (region, year, level, state, name) and dms."""
    con.execute(f"""
        CREATE TABLE delims AS
        SELECT regexp_extract(filename, '([a-z]+)_\\d{{4}}_', 1) AS region,
               CAST(regexp_extract(filename, '_(\\d{{4}})_', 1) AS INTEGER) AS year,
               'parlimen' AS level, state, {STRIP_CODE.format('parlimen')} AS name
        FROM read_parquet('{PATH_MAPS_DELIMS}*_parlimen.parquet', filename = true, union_by_name = true)
        UNION ALL
        SELECT regexp_extract(filename, '([a-z]+)_\\d{{4}}_', 1),
               CAST(regexp_extract(filename, '_(\\d{{4}})_', 1) AS INTEGER),
               'dun', state, {STRIP_CODE.format('dun')}
        FROM read_parquet('{PATH_MAPS_DELIMS}*_dun.parquet', filename = true, union_by_name = true)
    """)
    dupes = con.sql("SELECT region, year, level, state, name, count(*) AS n FROM delims GROUP BY ALL HAVING n > 1").df()
    assert dupes.empty, f"Seat names repeat within a delimitation, so name matching is ambiguous:\n{dupes}"
    con.execute(f"""
        CREATE TABLE dms AS
        SELECT CAST(regexp_extract(filename, '_(\\d{{4}})_', 1) AS INTEGER) AS year, state, dm
        FROM read_parquet('{PATH_MAPS_SUBDIVISIONS}*_dm.parquet', filename = true, union_by_name = true)
    """)


def build_lookup(con):
    """Return the lookup: one row per state x level x run of elections on the same delimitation."""
    delims_csv = pd.read_csv(DELIMS_TO_ELECTIONS, dtype=str)  # noqa: F841 (read by DuckDB below)
    con.execute(f"""
        CREATE TABLE contests AS
        SELECT DISTINCT date, election, state, seat,
               CASE WHEN seat LIKE 'P.%' THEN 'parlimen' ELSE 'dun' END AS level
        FROM '{PATH_RESULTS_HEADLINE}consol_stats.parquet'
    """)
    # One row per general/state election x state x level, dated from the results. GE rows in the
    # crosswalk are national (state 'Malaysia'), so they fan out to every state that polled.
    con.execute(f"""
        CREATE TABLE elections AS
        WITH polled AS (
            SELECT election, state, level, min(date) AS date
            FROM contests WHERE election != 'BY-ELECTION' GROUP BY ALL
        ),
        crosswalk AS (
            SELECT election, state, peninsular, sabah, sarawak FROM delims_csv
        )
        SELECT p.election, p.state, p.level, p.date, {REGION_SQL.replace('state', 'p.state')} AS region,
               CAST(CASE {REGION_SQL.replace('state', 'p.state')}
                        WHEN 'sabah' THEN c.sabah WHEN 'sarawak' THEN c.sarawak ELSE c.peninsular END AS INTEGER)
                   AS delimitation_year
        FROM polled p
        LEFT JOIN crosswalk c ON c.election = p.election
             AND c.state = CASE p.level WHEN 'parlimen' THEN 'Malaysia' ELSE p.state END
    """)
    missing = con.sql("SELECT * FROM elections WHERE coalesce(delimitation_year, 0) = 0 ORDER BY date").df()
    assert missing.empty, f"Elections in the results with no delimitation in DELIMS_TO_ELECTIONS:\n{missing}"

    return con.sql("""
        WITH runs AS (
            SELECT *, delimitation_year IS DISTINCT FROM
                      lag(delimitation_year) OVER (PARTITION BY state, level ORDER BY date) AS starts
            FROM elections
        ),
        numbered AS (SELECT *, sum(starts::INTEGER) OVER (PARTITION BY state, level ORDER BY date) AS run FROM runs),
        ranges AS (
            SELECT state, level, region, delimitation_year, min(date) AS valid_from,
                   arg_min(election, date) AS first_election, arg_max(election, date) AS last_election
            FROM numbered GROUP BY state, level, region, delimitation_year, run
        )
        SELECT state, level, delimitation_year, region, first_election, last_election, valid_from,
               lead(valid_from) OVER (PARTITION BY state, level ORDER BY valid_from) AS valid_until
        FROM ranges
        ORDER BY state, level, valid_from
    """).df()


def validate(con, lookup):
    """Every contest must resolve to exactly one delimitation that has its seat; report DM coverage."""
    con.register("lookup", lookup)
    resolved = con.sql(f"""
        SELECT c.*, l.region, l.delimitation_year,
               EXISTS (SELECT 1 FROM delims d WHERE d.region = l.region AND d.year = l.delimitation_year
                       AND d.level = c.level AND d.state = c.state AND d.name = {STRIP_CODE.format('c.seat')})
                   AS has_shape
        FROM contests c
        LEFT JOIN lookup l ON l.state = c.state AND l.level = c.level
             AND c.date >= l.valid_from AND (l.valid_until IS NULL OR c.date < l.valid_until)
    """).df()
    assert len(resolved) == con.sql("SELECT count(*) FROM contests").fetchone()[0], "A contest matched two ranges"
    bad = resolved[~resolved.has_shape.astype(bool)]
    assert bad.empty, f"{len(bad)} contests have no shape in their delimitation:\n{bad.head(30)}"
    n_by = (resolved.election == "BY-ELECTION").sum()
    print(f"  All {len(resolved):,.0f} contests ({n_by:,.0f} by-elections) have a shape in their delimitation")

    dm = con.sql(f"""
        WITH s AS (
            SELECT DISTINCT date, election, state, CASE WHEN seat LIKE 'P.%' THEN 'parlimen' ELSE 'dun' END AS level, dm
            FROM read_parquet('{PATH_LOCAL_LAKE}results_saluran/*_stats.parquet', union_by_name = true)
            WHERE split_part(dm, '/', 3) NOT SIMILAR TO '(00|UP).*'  -- early and postal votes have no area
        )
        SELECT s.date, s.election, s.state, l.delimitation_year, count(*) AS dms,
               count(d.dm) AS with_shape, bool_or(m.year IS NOT NULL) AS map_exists
        FROM s
        JOIN lookup l ON l.state = s.state AND l.level = s.level
             AND s.date >= l.valid_from AND (l.valid_until IS NULL OR s.date < l.valid_until)
        LEFT JOIN dms d ON d.state = s.state AND d.year = l.delimitation_year AND d.dm = s.dm
        LEFT JOIN (SELECT DISTINCT state, year FROM dms) m ON m.state = s.state AND m.year = l.delimitation_year
        GROUP BY ALL ORDER BY s.date, s.state
    """).df()
    gaps = dm[dm.map_exists & (dm.with_shape < dm.dms)]
    assert gaps.empty, f"DMs missing from a subdivision map that exists:\n{gaps}"
    covered = dm[dm.map_exists]
    print(f"  DM shapes: {covered.dms.sum():,.0f} DMs across {len(covered)} election x state results, all matched; "
          f"{len(dm) - len(covered)} older results have no DM map")


def make_lookup():
    """Build, check and save the lookup; return its local path."""
    con = duckdb.connect()
    read_maps(con)
    lookup = build_lookup(con)
    validate(con, lookup)
    os.makedirs(os.path.dirname(PATH_OUT), exist_ok=True)
    lookup.to_parquet(PATH_OUT, index=False, compression="zstd")
    print(f"  Saved {len(lookup):,.0f} rows -> {PATH_OUT}")
    return PATH_OUT


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    PATH = make_lookup()

    if "--skip-upload" in sys.argv:
        print("\n--skip-upload: not uploading")
    else:
        CLIENT = get_r2_client()
        BUCKET_LAKE = os.getenv("R2_BUCKET_LAKE")
        print("\nUploading to R2")
        UPLOADED = upload_bulk(
            CLIENT,
            BUCKET_LAKE,
            [(PATH, PATH.replace(PATH_LOCAL_LAKE, ""))],
            content_type="application/octet-stream",
        )
        write_lake_manifest(CLIENT, BUCKET_LAKE, UPLOADED)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"Duration: {datetime.now() - START}")
