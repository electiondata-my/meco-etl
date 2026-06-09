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
TEMPLATE_DIR = Path("template-catalogue")
TODAY = date.today().isoformat()

REGION_DISPLAY = {
    "peninsular": "Peninsular Malaysia",
    "sabah": "Sabah",
    "sarawak": "Sarawak",
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
        "zoom": {"desktop": 8.32, "mobile": 8.32},
        "center": {"desktop": [101.433, 5.756], "mobile": [101.433, 5.756]},
    },
    "sabah": {
        "zoom": {"desktop": 8.54, "mobile": 8.54},
        "center": {"desktop": [116.963, 6.524], "mobile": [116.963, 6.524]},
    },
    "sarawak": {
        "zoom": {"desktop": 8.60, "mobile": 8.60},
        "center": {"desktop": [114.558, 4.157], "mobile": [114.558, 4.157]},
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


def _get_file_size(stem, fmt):
    ext, subdir = FORMAT_EXTS_DELIMS[fmt]
    fp = PATH_MAPS / subdir / (stem + ext)
    return fp.stat().st_size if fp.exists() else None


def _build_catalogue(template, stem, region, year):
    """Return a filled-in catalogue dict for a single delimitation file."""
    gdf = gpd.read_parquet(PATH_MAPS_DELIMS / f"{stem}.parquet")
    n_objects = len(gdf)
    data_cols = [c for c in gdf.columns if c != "geometry"]
    n_attributes = len(data_cols)
    sample_rows = gdf[data_cols].to_dict(orient="records")

    cat_str = json.dumps(template)
    cat_str = cat_str.replace("YYYY", str(year))
    cat_str = cat_str.replace("UNIT", REGION_DISPLAY[region])
    cat = json.loads(cat_str)

    cat["data_as_of"] = str(year)

    base_url = f"https://lake.electiondata.my/maps/delimitations/{stem}"
    for fmt, (ext, _) in FORMAT_EXTS_DELIMS.items():
        cat["download"][fmt]["link"] = f"{base_url}{ext}"
        cat["download"][fmt]["n_objects"] = n_objects
        cat["download"][fmt]["n_attributes"] = n_attributes
        size = _get_file_size(stem, fmt)
        if size is not None:
            cat["download"][fmt]["size_bytes"] = size

    map_opts = cat["display_options"]["map"]
    map_opts["mapbox_key"] = stem
    map_opts["zoom"] = REGION_MAP_DISPLAY_DELIMS[region]["zoom"]
    map_opts["center"] = REGION_MAP_DISPLAY_DELIMS[region]["center"]

    cat["sample_data"] = sample_rows

    return cat


def make_delims_parlimen():
    """Generate catalogue JSONs for all parlimen delimitation files."""
    template = json.loads((TEMPLATE_DIR / "unit-yyyy-parlimen.json").read_text())
    out_dir = Path(PATH_LOCAL_INTERNAL) / "catalogue" / "maps"
    out_dir.mkdir(parents=True, exist_ok=True)

    for fp in sorted(g(str(PATH_MAPS_DELIMS / "*_parlimen.parquet"))):
        stem = Path(fp).stem
        region, year = stem.split("_")[:2]
        cat = _build_catalogue(template, stem, region, int(year))
        out_path = out_dir / f"{region}-{year}-parlimen.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cat, f, ensure_ascii=False)
        print(f"Written: {out_path}")


def make_delims_dun():
    """Generate catalogue JSONs for all DUN delimitation files."""
    template = json.loads((TEMPLATE_DIR / "unit-yyyy-dun.json").read_text())
    out_dir = Path(PATH_LOCAL_INTERNAL) / "catalogue" / "maps"
    out_dir.mkdir(parents=True, exist_ok=True)

    for fp in sorted(g(str(PATH_MAPS_DELIMS / "*_dun.parquet"))):
        stem = Path(fp).stem
        region, year = stem.split("_")[:2]
        cat = _build_catalogue(template, stem, region, int(year))
        out_path = out_dir / f"{region}-{year}-dun.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cat, f, ensure_ascii=False)
        print(f"Written: {out_path}")


def make_subdivisions_dm():
    """Generate catalogue JSONs for all DM subdivision files."""
    template = json.loads((TEMPLATE_DIR / "unit-yyyy-dm.json").read_text())
    out_dir = Path(PATH_LOCAL_INTERNAL) / "catalogue" / "maps"
    out_dir.mkdir(parents=True, exist_ok=True)

    for fp in sorted(g(str(PATH_MAPS_SUBDIVISIONS / "*_dm.parquet"))):
        stem = Path(fp).stem
        region, year = stem.split("_")[:2]

        gdf = gpd.read_parquet(fp)
        n_objects = len(gdf)
        data_cols = [c for c in gdf.columns if c != "geometry"]
        n_attributes = len(data_cols)
        sample_rows = gdf[data_cols].to_dict(orient="records")

        cat_str = json.dumps(template)
        cat_str = cat_str.replace("YYYY", str(year))
        cat_str = cat_str.replace("UNIT", REGION_DISPLAY[region])
        cat = json.loads(cat_str)

        cat["data_as_of"] = str(year)

        base_url = f"https://lake.electiondata.my/maps/subdivisions/{stem}"
        for fmt, ext in FORMAT_EXTS_SUBDIVS.items():
            cat["download"][fmt]["link"] = f"{base_url}{ext}"
            cat["download"][fmt]["n_objects"] = n_objects
            cat["download"][fmt]["n_attributes"] = n_attributes
            size_path = PATH_MAPS_SUBDIVISIONS / f"{stem}{ext}"
            if size_path.exists():
                cat["download"][fmt]["size_bytes"] = size_path.stat().st_size

        map_opts = cat["display_options"]["map"]
        map_opts["mapbox_key"] = stem
        map_opts["zoom"] = REGION_MAP_DISPLAY_SUBDIVS[region]["zoom"]
        map_opts["center"] = REGION_MAP_DISPLAY_SUBDIVS[region]["center"]

        cat["sample_data"] = sample_rows

        out_path = out_dir / f"{region}-{year}-dm.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cat, f, ensure_ascii=False)
        print(f"Written: {out_path}")


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
