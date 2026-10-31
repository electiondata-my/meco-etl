"""
Module: api_latest.py

Uploads the homepage's latest elections list and the OG images it references.
It:
- Reads latest.json (maintained by hand) and collects the og-image keys it references
- Skips referenced images that are already live; uploads the rest from local
- Refuses to upload latest.json if any referenced image is missing both live and locally
- Uploads latest.json, then purges the Cloudflare cache for it and the new images

Inputs:
- internal.electiondata.my/latest.json
- internal.electiondata.my/og-image/*.png

Outputs:
- internal.electiondata.my/latest.json uploaded to R2
- internal.electiondata.my/og-image/*.png uploaded to R2 (referenced images not yet live only)
"""

import os
import json as j
from datetime import datetime

import requests
from dotenv import load_dotenv

from helper import get_r2_client, upload_bulk, purge_cf_cache

load_dotenv()

PATH_LOCAL_INTERNAL = "internal.electiondata.my/"
BASE_URL = "https://internal.electiondata.my"


def get_image_keys():
    """Return the R2 keys of every OG image referenced by latest.json."""
    with open(f"{PATH_LOCAL_INTERNAL}latest.json", encoding="utf-8") as f:
        data = j.load(f)["data"]
    urls = [e[k] for e in data for k in ("img_light", "img_dark")]
    return sorted({u.replace(f"{BASE_URL}/", "") for u in urls})


def check_images(keys):
    """Return the image keys that are not live yet and must be uploaded from local.

    Images already live are skipped, so unchanged ones are never re-uploaded.

    Raises:
        RuntimeError: if any image is neither live nor local, since publishing
        latest.json would then show a broken image on the homepage
    """
    new, live, missing = [], [], []
    for key in keys:
        if requests.head(f"{BASE_URL}/{key}", timeout=30).status_code == 200:
            live.append(key)
        elif os.path.exists(f"{PATH_LOCAL_INTERNAL}{key}"):
            new.append(key)
        else:
            missing.append(key)
    print(f"\nImages: {len(new):,.0f} new (will upload), {len(live):,.0f} already live (skipped)")
    if missing:
        raise RuntimeError("Images missing locally and live:\n" + "\n".join(missing))
    return new


def upload_latest(client, bucket):
    """Upload any new OG images first, then latest.json, then purge both."""
    images = check_images(get_image_keys())
    upload_bulk(
        client,
        bucket,
        [(f"{PATH_LOCAL_INTERNAL}{k}", k) for k in images],
        max_workers=20,
        content_type="image/png",
    )
    upload_bulk(client, bucket, [(f"{PATH_LOCAL_INTERNAL}latest.json", "latest.json")])
    # new images too: a request made before upload leaves a cached 404 at the edge
    purge_cf_cache(["latest.json"] + images, BASE_URL)
    print(f"\nPurged cache for latest.json and {len(images):,.0f} new images")


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET = os.getenv("R2_BUCKET_INTERNAL")

    upload_latest(CLIENT, BUCKET)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
