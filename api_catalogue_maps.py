"""
Module: api_catalogue_maps.py

Generates catalogue JSON files for map delimitation and subdivision datasets by
combining templates with real file sizes, row counts, and sample data.

Inputs:
- template-catalogue/unit-yyyy-parlimen.json
- template-catalogue/unit-yyyy-dun.json
- template-catalogue/unit-yyyy-dm.json
- PATH_MAPS_DELIMS/*.parquet (geoparquet delimitation files)
- PATH_MAPS/ (all format variants: geojson, topojson, geoparquet, fgb, kml)
- PATH_MAPS_SUBDIVISIONS/*.parquet (geoparquet subdivision files, all formats flat)
- lake.electiondata.my/maps/ (staged files for exercises still at proposal stage)

Outputs:
- api.electiondata.my/catalogue/maps/delimitations/{region}_{year}_{type}.json
- api.electiondata.my/catalogue/maps/subdivisions/{region}_{year}_dm.json
"""

import os
import json
from pathlib import Path
from datetime import date, datetime
from glob import glob as g
import geopandas as gpd
from dotenv import load_dotenv
from helper import get_r2_client, upload_bulk, purge_cf_cache_prefix

load_dotenv()

PATH_MAPS = Path(os.getenv("PATH_MAPS"))
PATH_MAPS_DELIMS = Path(os.getenv("PATH_MAPS_DELIMS"))
PATH_MAPS_SUBDIVISIONS = Path(os.getenv("PATH_MAPS_SUBDIVISIONS"))
PATH_LOCAL_INTERNAL = "internal.electiondata.my/"
PATH_LOCAL_LAKE = Path("lake.electiondata.my/maps")
TEMPLATE_DIR = Path("template-catalogue")
TODAY = date.today().isoformat()

REGION_DISPLAY = {
    "peninsular": "Peninsular Malaysia",
    "sabah": "Sabah",
    "sarawak": "Sarawak",
}

# Exercises still at the public display stage are published with a proposal
# suffix on the filename (p1 = 1st proposal), and are staged under
# lake.electiondata.my/ rather than the map archives, which hold only gazetted
# boundaries. The catalogue entry itself stays unsuffixed, so the file stem and
# the catalogue slug are tracked separately throughout; the stem drives download
# links, file sizes and the mapbox key, the slug drives the output filename.
# Proposals also carry their own last_updated, since they are published off-cycle
# from the gazetted archives whose dates the templates hold.
PROPOSALS = {
    "sarawak-2026-parlimen": {
        "stem": "sarawak_2026_parlimen_p1",
        "level": "parlimen",
        "last_updated": "2026-09-03",
        "title": "2026 Delimitation of Sarawak (Parliament): 1st Proposal",
        "description": (
            "Boundaries of parliamentary constituencies in Sarawak as proposed in the 1st "
            "public display (Pameran 1) of the 2026 delimitation exercise. These boundaries "
            "are not yet gazetted, and may change before the exercise is concluded."
        ),
    },
    "sarawak-2026-dun": {
        "stem": "sarawak_2026_dun_p1",
        "level": "dun",
        "last_updated": "2026-09-03",
        "title": "2026 Delimitation of Sarawak (DUN): 1st Proposal",
        "description": (
            "Boundaries of DUN constituencies in Sarawak as proposed in the 1st public "
            "display (Pameran 1) of the 2026 delimitation exercise. These boundaries are "
            "not yet gazetted, and may change before the exercise is concluded."
        ),
    },
    "sarawak-2026-dm": {
        "stem": "sarawak_2026_dm_p1",
        "level": "dm",
        "last_updated": "2026-09-03",
        "title": "2026 Subdivision of Sarawak (Voting Districts): 1st Proposal",
        "description": (
            "Boundaries of voting districts in Sarawak as proposed in the 1st public "
            "display (Pameran 1) of the 2026 delimitation and accompanying subdivision "
            "exercise. These boundaries are not yet gazetted, and may change before the "
            "exercise is concluded."
        ),
    },
}

PROPOSAL_STEMS = {spec["stem"] for spec in PROPOSALS.values()}

# Only the proposal files carry the electorate, as stated in the public display.
FIELD_VOTERS_TOTAL = {
    "name": "voters_total",
    "title": "Total Voters",
    "description": (
        "[Integer] Number of registered voters in the constituency as stated in the EC's "
        "public display, e.g. 34197"
    ),
}

REGION_MAP_DISPLAY_DELIMS = {
    "peninsular": {
        "zoom": {"desktop": 5.8, "mobile": 5.8},
        "center": {"desktop": [102.9, 4.1], "mobile": [102.0, 4.13]},
    },
    "sabah": {
        "zoom": {"desktop": 6.5, "mobile": 6},
        "center": {"desktop": [117.354, 5.754], "mobile": [117.29, 5.80]},
    },
    "sarawak": {
        "zoom": {"desktop": 6.25, "mobile": 5.35},
        "center": {"desktop": [113.303, 2.913], "mobile": [112.7, 3.53]},
    },
}

REGION_MAP_DISPLAY_SUBDIVS = {
    "peninsular": {
        "zoom": {"desktop": 5.8, "mobile": 5.8},  # 8.32, 8.32
        "center": {
            "desktop": [102.9, 4.1],  # 101.433, 5.756
            "mobile": [102.0, 4.13],  # 101.433, 5.756
        },
    },
    "sabah": {
        "zoom": {"desktop": 6.5, "mobile": 6},  # 8.54, 8.54
        "center": {
            "desktop": [117.354, 5.754],  # 116.963, 6.524
            "mobile": [117.29, 5.80],  # 116.963, 6.524
        },
    },
    "sarawak": {
        "zoom": {"desktop": 6.25, "mobile": 5.35},  # 8.60, 8.60
        "center": {
            "desktop": [113.303, 2.913],  # 114.558, 4.157
            "mobile": [112.7, 3.53],  # 114.558, 4.157
        },
    },
}

FORMAT_EXTS_DELIMS = {
    "geojson": (".geojson", "geojson/delimitations"),
    "topojson": (".topojson", "topojson/delimitations"),
    "geoparquet": (".parquet", "geoparquet/delimitations"),
    "flatgeobuf": (".fgb", "fgb/delimitations"),
    "kml": (".kml", "kml/delimitations"),
}

FORMAT_EXTS_SUBDIVS = {
    "geojson": ".geojson",
    "topojson": ".topojson",
    "geoparquet": ".parquet",
    "flatgeobuf": ".fgb",
    "kml": ".kml",
}


def _map_path(stem, level, fmt):
    """Path to a map file on disk, staged lake dir for proposals, archive otherwise."""
    if level == "dm":
        ext = FORMAT_EXTS_SUBDIVS[fmt]
        archive = PATH_MAPS_SUBDIVISIONS / f"{stem}{ext}"
        kind = "subdivisions"
    else:
        ext, subdir = FORMAT_EXTS_DELIMS[fmt]
        archive = PATH_MAPS / subdir / f"{stem}{ext}"
        kind = "delimitations"

    if stem in PROPOSAL_STEMS:
        return PATH_LOCAL_LAKE / kind / f"{stem}{ext}"
    return archive


def _catalogue_targets(level):
    """{slug: stem} for a level; proposals override the gazetted file of the same slug."""
    root = PATH_MAPS_SUBDIVISIONS if level == "dm" else PATH_MAPS_DELIMS
    targets = {}

    for fp in sorted(g(str(root / f"*_{level}.parquet"))):
        stem = Path(fp).stem
        region, year = stem.split("_")[:2]
        targets[f"{region}-{year}-{level}"] = stem

    for slug, spec in PROPOSALS.items():
        if spec["level"] == level:
            targets[slug] = spec["stem"]

    return targets


def _build_catalogue(template, slug, stem, level):
    """Return a filled-in catalogue dict for a single map file."""
    region, year = slug.split("-")[:2]
    kind = "subdivisions" if level == "dm" else "delimitations"
    exts = (
        FORMAT_EXTS_SUBDIVS
        if level == "dm"
        else {fmt: ext for fmt, (ext, _) in FORMAT_EXTS_DELIMS.items()}
    )
    display = REGION_MAP_DISPLAY_SUBDIVS if level == "dm" else REGION_MAP_DISPLAY_DELIMS

    gdf = gpd.read_parquet(_map_path(stem, level, "geoparquet"))
    n_objects = len(gdf)
    data_cols = [c for c in gdf.columns if c != "geometry"]
    n_attributes = len(data_cols)
    sample_rows = gdf[data_cols].to_dict(orient="records")

    cat_str = json.dumps(template)
    cat_str = cat_str.replace("YYYY", year)
    cat_str = cat_str.replace("UNIT", REGION_DISPLAY[region])
    cat = json.loads(cat_str)

    cat["data_as_of"] = year

    if slug in PROPOSALS:
        cat["title"] = PROPOSALS[slug]["title"]
        cat["description"] = PROPOSALS[slug]["description"]
        cat["last_updated"] = PROPOSALS[slug]["last_updated"]

    if "voters_total" in data_cols:
        cat["fields"].append(FIELD_VOTERS_TOTAL)

    base_url = f"https://lake.electiondata.my/maps/{kind}/{stem}"
    for fmt, ext in exts.items():
        cat["download"][fmt]["link"] = f"{base_url}{ext}"
        cat["download"][fmt]["n_objects"] = n_objects
        cat["download"][fmt]["n_attributes"] = n_attributes
        size_path = _map_path(stem, level, fmt)
        if size_path.exists():
            cat["download"][fmt]["size_bytes"] = size_path.stat().st_size

    map_opts = cat["display_options"]["map"]
    map_opts["mapbox_key"] = stem
    map_opts["zoom"] = display[region]["zoom"]
    map_opts["center"] = display[region]["center"]

    cat["sample_data"] = sample_rows

    return cat


def _make_level(level, template_name):
    """Generate catalogue JSONs for every dataset at a given map level."""
    template = json.loads((TEMPLATE_DIR / template_name).read_text())
    out_dir = Path(PATH_LOCAL_INTERNAL) / "catalogue" / "maps"
    out_dir.mkdir(parents=True, exist_ok=True)

    for slug, stem in sorted(_catalogue_targets(level).items()):
        cat = _build_catalogue(template, slug, stem, level)
        out_path = out_dir / f"{slug}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cat, f, ensure_ascii=False)
        print(f"Written: {out_path}")


def make_delims_parlimen():
    """Generate catalogue JSONs for all parlimen delimitation files."""
    _make_level("parlimen", "unit-yyyy-parlimen.json")


def make_delims_dun():
    """Generate catalogue JSONs for all DUN delimitation files."""
    _make_level("dun", "unit-yyyy-dun.json")


def make_subdivisions_dm():
    """Generate catalogue JSONs for all DM subdivision files."""
    _make_level("dm", "unit-yyyy-dm.json")


def upload_catalogue_maps(client, bucket, file_pattern="catalogue/maps/*"):
    """Upload catalogue map JSONs to R2."""
    files = g(f"{PATH_LOCAL_INTERNAL}{file_pattern}.json")
    print(f"\nUploading {len(files):,.0f} files to R2")
    files_to_upload = sorted(
        [(f, f.replace(PATH_LOCAL_INTERNAL, "").replace("/maps", "")) for f in files]
    )
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def purge_catalogue_maps_cache(prefix="catalogue/"):
    """Purge Cloudflare cache for catalogue map JSONs by URL prefix."""
    full_prefix = f"{PATH_LOCAL_INTERNAL}{prefix}"
    print(f"\nPurging cache prefix: {full_prefix}")
    purge_cf_cache_prefix([full_prefix])


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET = os.getenv("R2_BUCKET_INTERNAL")

    make_delims_parlimen()
    make_delims_dun()
    make_subdivisions_dm()
    upload_catalogue_maps(CLIENT, BUCKET)
    purge_catalogue_maps_cache()

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
