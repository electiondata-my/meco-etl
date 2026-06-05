"""Generate seat data files for API."""

import os
import json as j
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv

from helper import generate_slug, get_center_and_zoom

load_dotenv()
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")
PATH_MAPS_DELIMS = os.getenv("PATH_MAPS_DELIMS")
PATH_LOCAL_INTERNAL = "internal.electiondata.my/"


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

    # ----- sf (seat frame): contains all results by seat; ff (filter frame): contains all lineage information by seat; left-joining allows us to pull out everything tagged to a current_seat -----
    sf = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}elections_by_seat.parquet")
    sf.date = pd.to_datetime(sf.date).dt.date.astype(str)
    ff = pd.read_parquet(f"{PATH_LOCAL_INTERNAL}lineage/filter.parquet")
    ff.current_seat = ff.current_seat.apply(generate_slug)
    ff.seat = ff.seat + ", " + ff.state
    sf = pd.merge(sf, ff, on=["election_name", "state", "seat"], how="left")

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

    # ----- stitch everything together into a single JSON object -----

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
        tfl = lf[lf.slug == slug].copy().drop("slug", axis=1)
        tfl = tfl.to_dict(orient="records")
        tfl = [
            {k: (None if pd.isna(v) else v) for k, v in record.items()} for record in tfl
        ]  # proper JSON null
        data["results"] = tf + tfl
        data["results"].sort(key=lambda x: x.get("date", ""), reverse=True)

        data["map_plot"]["zoom"] = vf[slug]["zoom"]
        data["map_plot"]["center"] = vf[slug]["center"]
        data["map_plot"]["polygons"] = pf[slug]
        data["map_lineage"] = mf[slug]

        with open(f"{PATH_LOCAL_INTERNAL}seats/current/{slug}.json", "w", encoding="utf-8") as f:
            j.dump(data, f)


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    make_seats()

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
