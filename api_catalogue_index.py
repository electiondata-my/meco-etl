"""
Module: api_catalogue_index.py

Uploads the catalogue index JSON to the internal R2 bucket and purges its cache.
It:
- Uploads the local catalogue index to R2
- Purges the Cloudflare cache for the uploaded file

Inputs:
- internal.electiondata.my/catalogue/index.json

Outputs:
- internal.electiondata.my/catalogue/index.json uploaded to R2
"""

import os
from datetime import datetime
from dotenv import load_dotenv
from helper import get_r2_client, upload_bulk, purge_cf_cache

load_dotenv()

PATH_LOCAL_INTERNAL = "internal.electiondata.my/"
CATALOGUE_INDEX_KEY = "catalogue/index.json"
BASE_URL = "https://internal.electiondata.my"


def upload_catalogue_index(client, bucket):
    """Upload the catalogue index JSON to R2."""
    source = f"{PATH_LOCAL_INTERNAL}{CATALOGUE_INDEX_KEY}"
    print(f"\nUploading {CATALOGUE_INDEX_KEY} to R2")
    upload_bulk(client, bucket, [(source, CATALOGUE_INDEX_KEY)])


def purge_catalogue_index_cache():
    """Purge Cloudflare cache for the catalogue index JSON."""
    print(f"\nPurging cache: {BASE_URL}/{CATALOGUE_INDEX_KEY}")
    purge_cf_cache([CATALOGUE_INDEX_KEY], BASE_URL)


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET = os.getenv("R2_BUCKET_INTERNAL")

    upload_catalogue_index(CLIENT, BUCKET)
    purge_catalogue_index_cache()

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
