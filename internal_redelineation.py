"""
Module: internal_redelineation.py

This module uploads redelineation data for internal.electiondata.my.
It:
- Uploads processed redelineation JSON files to R2
- Purges Cloudflare cache for redelineation files

Inputs:
- internal.electiondata.my/redelineation/*.json

Outputs:
- Redelineation JSON files uploaded to R2
"""

import os
from glob import glob as g
from datetime import datetime

from dotenv import load_dotenv

from helper import get_r2_client, upload_bulk, purge_cf_cache_prefix

load_dotenv()

PATH_LOCAL_INTERNAL = "internal.electiondata.my/"


# TODO: Add function to generate redelineation JSON files


def upload_redelineation_jsons(client, bucket, file_pattern="redelineation/*"):
    """Upload redelineation JSON files to R2."""
    files = g(f"{PATH_LOCAL_INTERNAL}{file_pattern}.json")
    print(f"\nUploading {len(files):,.0f} files to R2")
    files_to_upload = sorted([(f, f.replace(PATH_LOCAL_INTERNAL, "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def purge_redelineation_cache(prefix="redelineation/"):
    """Purge Cloudflare cache for redelineation JSON files by URL prefix."""
    full_prefix = f"{PATH_LOCAL_INTERNAL}{prefix}"
    print(f"\nPurging cache prefix: {full_prefix}")
    purge_cf_cache_prefix([full_prefix])


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET = os.getenv("R2_BUCKET_INTERNAL")

    upload_redelineation_jsons(CLIENT, BUCKET)
    purge_redelineation_cache()

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
