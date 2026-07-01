"""
Module: api_seats_current.py

Processes and uploads current seat data for the public API at api.electiondata.my.
It:
- Reads and transforms seat data from internal parquets (results, lineage, demographics, map)
- Generates a dropdown JSON and a consolidated all.json for internal use
- Generates per-seat JSONs containing only the results key for the public API
- Uploads all outputs to the appropriate R2 buckets

Inputs:
- internal.electiondata.my/elections_by_seat.parquet
- internal.electiondata.my/lineage/*.parquet
- internal.electiondata.my/seats/current/all.json

Outputs:
- internal.electiondata.my/seats/current/dropdown.json uploaded to R2
- internal.electiondata.my/seats/current/all.json uploaded to R2
- api.electiondata.my/v1/seats/current/{slug}.json uploaded to R2
"""

import os
import json as j
from glob import glob as g
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv

from helper import (
    generate_slug,
    get_voter_pyramid_from_vr,
    get_age_x_ethnicity_from_vr,
    get_center_and_zoom,
    get_r2_client,
    upload_bulk,
    purge_cf_cache_prefix,
)

load_dotenv()
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")
PATH_MAPS_DELIMS = os.getenv("PATH_MAPS_DELIMS")
PATH_VR = os.getenv("PATH_VR")
PATH_LOCAL_INTERNAL = "internal.electiondata.my/"
PATH_LOCAL_API = "api.electiondata.my/v1/seats/current/"


def make_seats():
    """Generate seat data files for API."""
    data = {
        "map_plot": {
            "zoom": 0,
            "center": [0, 0],
            "polygons": {},
        },
        "map_lineage": [],
        "results": [],
    }

    col_api_seat = [
        "election_name",
        "seat",
        "state",
        "date",
        "party",
        "party_uid",
        "coalition",
        "coalition_uid",
        "name",
        "majority",
        "majority_perc",
        "voter_turnout",
        "voter_turnout_perc",
    ]

    # ----- master list of slugs derived from seats currently in effect -----
    df = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}elections_by_seat.parquet")
    df = df[  # latest elections at federal and state level
        (df.election_name == "GE-15")
        | (df.election_name == "SE-15")
        | ((df.state == "Sarawak") & (df.election_name == "SE-12"))
    ][["seat", "slug", "type"]]
    slugs = df.slug.tolist()
    data = {"data": df.to_dict(orient="records")}
    with open(f"{PATH_LOCAL_INTERNAL}seats/current/dropdown.json", "w", encoding="utf-8") as f:
        j.dump(data, f)
    with open(f"{PATH_LOCAL_API}dropdown.json", "w", encoding="utf-8") as f:
        j.dump({"seats": data["data"]}, f)

    # ----- sf (seat frame): contains all results by seat; ff (filter frame): contains all lineage information by seat; left-joining allows us to pull out everything tagged to a current_seat -----
    sf = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}elections_by_seat.parquet")
    sf.date = pd.to_datetime(sf.date).dt.date.astype(str)
    _pending_elections = set(sf[sf.party.isna()].election_name.unique())
    ff = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}lineage/filter.parquet")
    ff.current_seat = ff.current_seat.apply(generate_slug)
    ff.seat = ff.seat + ", " + ff.state

    # add dates to ff to prepare for by-elections (where seat-election is not unique)
    len_before = len(ff)
    ff = pd.merge(ff, sf[["election_name", "state", "seat", "date"]], how="left")
    assert len(ff) == len_before, "Explosion of filter frame after adding dates!"

    # add by-elections into ff, based on sf
    by_elections = sf[sf.election_name == "By-Election"][
        ["election_name", "state", "seat", "date"]
    ].drop_duplicates()
    main_elections = sf[sf.election_name != "By-Election"][
        ["election_name", "state", "seat", "date"]
    ].drop_duplicates()
    new_ff_rows = []

    for _, by_row in by_elections.iterrows():
        # find all main elections for the same seat+state prior to this by-election
        candidates = main_elections[
            (main_elections.seat == by_row.seat)
            & (main_elections.state == by_row.state)
            & (main_elections.date < by_row.date)
        ]
        if candidates.empty:
            continue
        # take the most recent one
        prev = candidates.loc[candidates.date.idxmax()]
        # check if that combo exists in ff
        match = ff[
            (ff.election_name == prev.election_name)
            & (ff.state == prev.state)
            & (ff.seat == prev.seat)
            & (ff.date == prev.date)
        ]
        if match.empty:
            continue
        # inherit current_seat and insert row for the by-election
        new_ff_rows.append(
            {
                "current_seat": match.iloc[0].current_seat,
                "election_name": by_row.election_name,
                "state": by_row.state,
                "seat": by_row.seat,
                "date": by_row.date,
            }
        )

    if new_ff_rows:
        ff = pd.concat([ff, pd.DataFrame(new_ff_rows)], ignore_index=True)
        ff = ff.sort_values(by=["current_seat", "date"], ignore_index=True)

    sf = pd.merge(sf, ff, on=["election_name", "state", "seat", "date"], how="left")
    # pending elections are not in the lineage filter; the seat being contested IS the current seat
    pending_mask = sf["election_name"].isin(_pending_elections) & sf["current_seat"].isna()
    sf.loc[pending_mask, "current_seat"] = sf.loc[pending_mask, "slug"]

    # ----- lf (lineage frame): contains all lineage descriptions by seat -----
    lf = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}lineage/desc.parquet")
    lf = lf[lf.change_en != "Unchanged"]
    lf = lf[~lf.change_en.str.contains("was not renamed, its boundaries were changed")]
    lf.seat = lf.seat.apply(generate_slug)
    lf = lf.rename(columns={"seat": "slug"})

    # ----- pf (plot frame): contains instructions for which polygons to show on the map -----
    pf = (
        pd.concat(
            [
                pd.read_parquet(f"{PATH_LOCAL_INTERNAL}lineage/parlimen_geo.parquet").rename(
                    columns={"parlimen": "seat"}
                ),
                pd.read_parquet(f"{PATH_LOCAL_INTERNAL}lineage/dun_geo.parquet").rename(
                    columns={"dun": "seat"}
                ),
            ]
        )
        .rename(columns={"current_seat": "slug"})
        .drop(columns=["n_duns", "duns", "parlimen"])
    )
    pf["delimitation"] = "peninsular_" + pf.year.astype(str) + "_parlimen"
    for s in ["Sabah", "Sarawak"]:
        pf.loc[pf.state == s, "delimitation"] = pf.loc[pf.state == s, "delimitation"].str.replace(
            "peninsular", s.lower()
        )
    pf.loc[pf.slug.str.startswith("N"), "delimitation"] = pf.loc[
        pf.slug.str.startswith("N"), "delimitation"
    ].str.replace("parlimen", "dun")
    mask_kl = pf.slug.str.startswith(tuple(f"P.{str(i).zfill(3)}" for i in range(114, 125)))
    pf.loc[mask_kl, "state"] = "W.P. Kuala Lumpur"
    pf.loc[pf.slug.str.startswith("P.125"), "state"] = "W.P. Putrajaya"
    pf.loc[pf.slug.str.startswith("P.166"), "state"] = "W.P. Labuan"
    pf.slug = pf.slug + ", " + pf.state
    pf = pf.drop(columns=["state"])
    pf.slug = pf.slug.apply(generate_slug)
    assert (pf.groupby(["slug", "year"])["delimitation"].nunique() == 1).all()
    pf = {
        slug: {
            str(year): [year_group["delimitation"].iloc[0], year_group["seat"].tolist()]
            for year, year_group in group.groupby("year")
        }
        for slug, group in pf.groupby("slug")
    }

    # ----- vf (visual frame): contains the appropriate zoom level and center for the map -----
    latest_delims = [
        f"{PATH_MAPS_DELIMS}peninsular_2018_parlimen.parquet",
        f"{PATH_MAPS_DELIMS}peninsular_2018_dun.parquet",
        f"{PATH_MAPS_DELIMS}sabah_2019_parlimen.parquet",
        f"{PATH_MAPS_DELIMS}sabah_2019_dun.parquet",
        f"{PATH_MAPS_DELIMS}sarawak_2015_parlimen.parquet",
        f"{PATH_MAPS_DELIMS}sarawak_2015_dun.parquet",
    ]

    vf = get_center_and_zoom(latest_delims).rename(columns={"seat": "slug"})
    vf.slug = vf.slug + ", " + vf.state
    vf.slug = vf.slug.apply(generate_slug)
    vf = vf.drop(columns=["state"])
    vf = vf.set_index("slug")[["center", "zoom"]].to_dict(orient="index")

    # ----- mf (map lineage frame): contains all information shown in the geo-lineage table -----
    mf = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}lineage/parlimen_geo.parquet").rename(
        columns={"current_seat": "slug"}
    )
    mask_kl = mf.slug.str.startswith(tuple(f"P.{str(i).zfill(3)}" for i in range(114, 125)))
    mf.loc[mask_kl, "state"] = "W.P. Kuala Lumpur"
    mf.loc[mf.slug.str.startswith("P.125"), "state"] = "W.P. Putrajaya"
    mf.loc[mf.slug.str.startswith("P.166"), "state"] = "W.P. Labuan"
    mf.slug = mf.slug + ", " + mf.state
    mf = mf.drop(columns=["state"])
    mf.slug = mf.slug.apply(generate_slug)
    mf_par = {
        slug: group.drop(columns="slug").to_dict(orient="records")
        for slug, group in mf.groupby("slug")
    }

    mf = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}lineage/dun_geo.parquet").rename(
        columns={"current_seat": "slug"}
    )
    mf.slug = mf.slug + ", " + mf.state
    mf = mf.drop(columns=["state"])
    mf.slug = mf.slug.apply(generate_slug)
    mf_dun = {
        slug: group.drop(columns="slug").to_dict(orient="records")
        for slug, group in mf.groupby("slug")
    }

    mf = mf_par | mf_dun
    mf = {
        slug: [{k: (None if pd.isna(v) else v) for k, v in record.items()} for record in records]
        for slug, records in mf.items()
    }

    # ----- demographic data -----
    pyramid = get_voter_pyramid_from_vr(f"{PATH_VR}ge15_2022.parquet", reference_year=2022)
    heatmap = get_age_x_ethnicity_from_vr(f"{PATH_VR}ge15_2022.parquet", reference_year=2022)
    pyramid_jhr_se16 = get_voter_pyramid_from_vr(
        f"{PATH_VR}jhr_se16_2026.parquet", reference_year=2026
    )
    heatmap_jhr_se16 = get_age_x_ethnicity_from_vr(
        f"{PATH_VR}jhr_se16_2026.parquet", reference_year=2026
    )

    # ----- stitch everything together into a single JSON object -----

    all_data = {}
    for slug in slugs:
        # get election results
        seat_type = sf[sf.slug == slug].type.iloc[0]
        tf = (
            sf[(sf.current_seat == slug) & (sf.type == seat_type)]
            .copy()[col_api_seat]
            .sort_values(by="date", ascending=False)
        )
        tf.seat = tf.seat.str.split(",").str[0]
        tf = tf.to_dict(orient="records")
        tf = [
            {k: (None if pd.isna(v) else v) for k, v in record.items()} for record in tf
        ]  # proper JSON null

        # get lineage descriptions, combine, and sort by date to insert lineage at the right spot (before or after the election)
        tfl = lf[lf.slug == slug].copy().drop(columns=["slug"])
        tfl = tfl.to_dict(orient="records")
        tfl = [
            {k: (None if pd.isna(v) else v) for k, v in record.items()} for record in tfl
        ]  # proper JSON null
        results = tf + tfl
        results.sort(key=lambda x: x.get("date", ""), reverse=True)

        # demographic data
        is_johor_se16 = slug[0] == "n" and "-johor" in slug
        pyramid_source = pyramid_jhr_se16 if is_johor_se16 else pyramid
        heatmap_source = heatmap_jhr_se16 if is_johor_se16 else heatmap

        tfp = (
            pyramid_source[pyramid_source.slug == slug]
            .copy()
            .drop(columns=["slug"])
            .pivot(index="age", columns="sex", values="voters")
            .reset_index()
        )
        tfh = (
            heatmap_source[heatmap_source.slug == slug]
            .copy()
            .drop(columns=["slug"])
            .set_index("ethnicity")
            .transpose()
        )

        all_data[slug] = {
            "map_plot": {
                "zoom": vf[slug]["zoom"] + 0.75,
                "center": vf[slug]["center"],
                "polygons": pf[slug],
            },
            "map_lineage": mf[slug],
            "results": results,
            "pyramid": {
                "ages": tfp["age"].tolist(),
                "male": tfp["Male"].tolist(),
                "female": tfp["Female"].tolist(),
            },
            "heatmap": {
                "ages": tfh.index.tolist(),
                **{col: tfh[col].tolist() for col in tfh.columns},
            },
        }

    with open(f"{PATH_LOCAL_INTERNAL}seats/current/all.json", "w", encoding="utf-8") as f:
        j.dump(all_data, f)


def make_seats_api():
    """Generate per-seat JSON files for the public API containing only results.

    Inputs:
    - internal.electiondata.my/seats/current/all.json

    Outputs:
    - api.electiondata.my/v1/seats/current/{slug}.json (results up to first redelineation change)
    - api.electiondata.my/v1/seats/current/{slug}-lineage.json (full results including lineage)
    """
    os.makedirs(PATH_LOCAL_API, exist_ok=True)

    with open(f"{PATH_LOCAL_INTERNAL}seats/current/all.json", encoding="utf-8") as f:
        all_data = j.load(f)

    for slug, seat_data in all_data.items():
        results = seat_data["results"]

        cutoff = next(
            (i for i, entry in enumerate(results) if "change_en" in entry),
            len(results),
        )

        with open(f"{PATH_LOCAL_API}{slug}.json", "w", encoding="utf-8") as f:
            j.dump({"results": results[:cutoff]}, f)

        with open(f"{PATH_LOCAL_API}{slug}-lineage.json", "w", encoding="utf-8") as f:
            j.dump({"results": results}, f)

    print(
        f"Wrote {len(all_data):,.0f} seat JSONs ({len(all_data):,.0f} standard + {len(all_data):,.0f} lineage)"
    )


def upload_seats_jsons(client, bucket, file_pattern="seats/current/*"):
    """Upload data files matching pattern to R2."""
    files = g(f"{PATH_LOCAL_INTERNAL}{file_pattern}.json")
    print(f"\nUploading {len(files):,.0f} files to R2")
    files_to_upload = sorted([(f, f.replace(PATH_LOCAL_INTERNAL, "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def upload_seats_api_jsons(client, bucket):
    """Upload API seat JSON files to R2."""
    files = g(f"{PATH_LOCAL_API}*.json")
    print(f"\nUploading {len(files):,.0f} API seat files to R2")
    files_to_upload = sorted([(f, f.replace("api.electiondata.my/", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def purge_seats_cache(prefix="seats/current/"):
    """Purge Cloudflare cache for seat JSON files by URL prefix."""
    full_prefix = f"{PATH_LOCAL_INTERNAL}{prefix}"
    print(f"\nPurging cache prefix: {full_prefix}")
    purge_cf_cache_prefix([full_prefix])


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET_INTERNAL = os.getenv("R2_BUCKET_INTERNAL")
    BUCKET_API = os.getenv("R2_BUCKET_API")

    make_seats()
    upload_seats_jsons(CLIENT, BUCKET_INTERNAL)
    purge_seats_cache()

    make_seats_api()
    upload_seats_api_jsons(CLIENT, BUCKET_API)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
