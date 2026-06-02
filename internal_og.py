"""
Script to generate and upload OG (Open Graph) images for each parliamentary constituency and state.

Steps:

    1. Read geometry and attribute data (e.g., constituency boundaries and metadata).
    2. For each row (constituency or state), generate a corresponding GeoPandas GeoDataFrame.
    3. For each item not already processed, calculate proper map bounding box with appropriate margins.
    4. Calculate map center and zoom level for visualization.
    5. Render map visualization using Plotly, highlighting the constituency/state area.
    6. Save the resulting image to the `api` directory, avoiding recomputation when possible.
    7. Upload generated images in bulk to S3 storage using helper scripts.

Notes:
    - Uses Plotly for map rendering.
    - Utilizes helper functions for slug generation and S3 uploads.
    - Skip regeneration if image for a given area already exists in the output folder.
"""

import os
from glob import glob
from math import log2
from datetime import datetime
import geopandas as gpd
import pandas as pd
import plotly.express as px
import plotly.io as pio
from dotenv import load_dotenv

from helper import generate_slug, get_r2_client, upload_bulk

load_dotenv()

PATH_MAPS_DELIMS = os.getenv("PATH_MAPS_DELIMS")
PATH_LOCAL_INTERNAL = "internal.electiondata.my/"


def calculate_zoom(lon_span, lat_span):
    """Calculate a suitable zoom level (for map display)
    based on the spans of longitude and latitude.
    Returns a value generally between 0-20 for web mapping.
    """
    scale = max(lon_span / 360, lat_span / 180)
    zoom_value = -log2(scale) + 1
    return min(max(zoom_value, 0), 20)


def make_og_image(feature_row, crs=4326, feature_type="parlimen"):
    """Generate center, zoom, and slug for an area,
    checking if the image already exists. Returns tuple
    (slug, center, zoom) or None if already done.
    Generates both light and dark versions of the image.
    """
    done = glob(f"{PATH_LOCAL_INTERNAL}og-image/*.png")
    done = [x.replace(f"{PATH_LOCAL_INTERNAL}og-image/", "").replace(".png", "") for x in done]

    feature_gdf = gpd.GeoDataFrame([feature_row], crs=crs)
    feature_slug = (
        generate_slug(feature_gdf[feature_type].iloc[0])
        + "-"
        + generate_slug(feature_gdf.state.iloc[0])
    )

    minx, miny, maxx, maxy = feature_gdf.total_bounds
    x_margin = (maxx - minx) * 0.1
    y_margin = (maxy - miny) * 0.1
    minx -= x_margin
    maxx += x_margin
    miny -= y_margin
    maxy += y_margin

    center_dict = {"lon": (minx + maxx) / 2, "lat": (miny + maxy) / 2}
    zoom_level = calculate_zoom(maxx - minx, maxy - miny)

    # Check if both light and dark versions already exist
    if feature_slug in done and f"{feature_slug}-dark" in done:
        return feature_slug, center_dict, zoom_level, "already_exists"

    # Generate light version
    if feature_slug not in done:
        fig_light = px.choropleth_map(
            feature_gdf,
            geojson=feature_gdf.geometry.__geo_interface__,
            locations=feature_gdf.index,
            color_discrete_sequence=["rgba(255,0,0,0.3)"],
            opacity=0.5,
            center=center_dict,
            zoom=zoom_level,
            map_style="carto-positron",
        )

        fig_light.update_traces(marker_line_width=2, marker_line_color="red")

        fig_light.update_layout(
            showlegend=False, margin={"r": 0, "t": 0, "l": 0, "b": 0}, width=1200, height=630
        )

        pio.write_image(
            fig_light,
            f"{PATH_LOCAL_INTERNAL}og-image/{feature_slug}.png",
            width=1200,
            height=630,
            scale=1,
        )

    # Generate dark version
    if f"{feature_slug}-dark" not in done:
        fig_dark = px.choropleth_map(
            feature_gdf,
            geojson=feature_gdf.geometry.__geo_interface__,
            locations=feature_gdf.index,
            color_discrete_sequence=["rgba(239,68,68,0.6)"],  # Brighter red fill
            opacity=0.7,  # Increased opacity
            center=center_dict,
            zoom=zoom_level,
            map_style="carto-darkmatter",
        )

        fig_dark.update_traces(marker_line_width=2, marker_line_color="rgb(248,113,113)")

        fig_dark.update_layout(
            showlegend=False, margin={"r": 0, "t": 0, "l": 0, "b": 0}, width=1200, height=630
        )

        pio.write_image(
            fig_dark,
            f"{PATH_LOCAL_INTERNAL}og-image/{feature_slug}-dark.png",
            width=1200,
            height=630,
            scale=1,
        )

    return feature_slug, center_dict, zoom_level, "newly_generated"


def upload_og_images(client, bucket, file_pattern="og-image/*"):
    """Upload PNG images (or other data files) from `api/` to R2 in bulk."""
    files = glob(f"{PATH_LOCAL_INTERNAL}{file_pattern}")
    print(f"\nUploading {len(files):,.0f} files to R2")
    files_to_upload = sorted([(f, f.replace(f"{PATH_LOCAL_INTERNAL}", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    print("\nGenerating OG images...")
    res = pd.DataFrame(columns=["slug", "center_lat", "center_lon", "zoom", "status"])

    for filename in [
        "peninsular_2018_parlimen",
        "peninsular_2018_dun",
        "sabah_2019_parlimen",
        "sabah_2019_dun",
        "sarawak_2015_parlimen",
        "sarawak_2015_dun",
    ]:
        gf = gpd.read_parquet(f"{PATH_MAPS_DELIMS}{filename}.parquet")
        gf = gf.to_crs(epsg=4326)
        df = pd.DataFrame(columns=res.columns)
        area_type = "dun" if "dun" in filename else "parlimen"
        for idx, row in gf.iterrows():
            result = make_og_image(row, gf.crs, area_type)
            slug, center, zoom, status = result
            df.loc[len(df)] = [
                slug,
                round(center["lat"], 6),
                round(center["lon"], 6),
                zoom,
                status,
            ]
        res = df.copy() if len(res) == 0 else pd.concat([res, df])
    res.to_csv("logs/og_images_generated.csv", index=False)

    print("\nUploading OG images to R2...")
    CLIENT = get_r2_client()
    BUCKET = os.getenv("R2_BUCKET_INTERNAL")
    upload_og_images(CLIENT, BUCKET)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
