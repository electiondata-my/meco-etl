"""
Module: api_jhr_se16.py

Runs the full pipeline for the internal site, but restricts the public API upload to the Johor SE-16 election only.
It:
- Regenerates and uploads the internal candidates, parties and seats/current files (dropdown + all.json)
- Regenerates the internal election files, but uploads only elections/all.json and elections/Johor/SE-16.json
- Regenerates and uploads the internal results files for the SE-16 polling date only
- Derives the SE-16 contestants (candidates, parties, coalitions, seats) from consol_ballots
- Uploads to the API bucket only: the SE-16 candidates, their parties/coalitions (Johor-dun), the 56 Johor duns, the Johor SE-16 election, and the 56 SE-16 results files

Inputs:
- {PATH_RESULTS_HEADLINE}consol_ballots.parquet
- {PATH_RESULTS_HEADLINE}lookup_party.parquet
- {PATH_RESULTS_HEADLINE}lookup_coalition.parquet

Outputs:
- internal.electiondata.my/{candidates,parties,seats/current}/{dropdown,all}.json uploaded to R2
- internal.electiondata.my/elections/{all.json,Johor/SE-16.json} uploaded to R2
- internal.electiondata.my/results/{seat}/2026-07-11.json uploaded to R2
- api.electiondata.my/v1/candidates/{uid}.json uploaded to R2 (SE-16 candidates only)
- api.electiondata.my/v1/{parties,coalitions}/{uid}/Johor-dun.json uploaded to R2 (SE-16 parties only)
- api.electiondata.my/v1/seats/current/{slug}.json uploaded to R2 (56 Johor duns only)
- api.electiondata.my/v1/elections/Johor/SE-16-*.json uploaded to R2
- api.electiondata.my/v1/results/{seat}/2026-07-11.json copied to R2 (56 files)
"""

import os
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv

from helper import generate_slug, get_r2_client, upload_bulk, purge_cf_cache

from api_candidates import (
    make_candidates_df,
    make_candidates_jsons,
    upload_candidates_jsons,
    purge_candidates_cache,
    make_api_candidates_jsons,
    upload_api_candidates_jsons,
)
from api_parties import (
    make_parties_df,
    make_parties_jsons,
    upload_parties_jsons,
    purge_parties_cache,
    make_api_parties_jsons,
    PATH_LOCAL_API_PARTIES,
    PATH_LOCAL_API_COALITIONS,
)
from api_elections import (
    make_election_stats,
    make_elections_by_seat,
    make_elections_jsons,
    sync_dropdown_json_to_api,
    PATH_LOCAL_API as PATH_API_ELECTIONS,
)
from api_results import make_results, PATH_LOCAL_INTERNAL, INTERNAL_BASE_URL
from api_seats_current import (
    make_seats,
    upload_seats_jsons,
    purge_seats_cache,
    make_seats_api,
    PATH_LOCAL_API as PATH_API_SEATS,
)

load_dotenv()
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")

DATE = "2026-07-11"
ELECTION = "SE-16"
STATE = "Johor"


def get_se16_scope():
    """Resolve everything contesting Johor SE-16 from the consolidated ballots.

    Party and coalition uids are normalised the same way api_parties.py does it (the
    numeric prefix is the stable key), because that is the uid the API folders use.

    Returns:
        dict with keys: candidates (uids), parties (uids), coalitions (uids),
        seats (slugs, as used by the seats API), seat_names ("N.01 Buloh Kasap, Johor",
        as used by the results paths)
    """
    df = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}consol_ballots.parquet")
    df = df[(df.date.astype(str) == DATE) & (df.election == ELECTION) & (df.state == STATE)]

    pf = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}lookup_party.parquet")
    pf["prefix"] = pf.party_uid.str.split("-").str[0]
    pf = pf.drop_duplicates(subset=["prefix"], keep="last")
    map_prefix_party = dict(zip(pf.prefix, pf.party_uid))

    cf = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}lookup_coalition.parquet")
    cf["prefix"] = cf.coalition_uid.str.split("-").str[0]
    cf = cf.drop_duplicates(subset=["prefix"], keep="last")
    map_prefix_coalition = dict(zip(cf.prefix, cf.coalition_uid))

    scope = {
        "candidates": sorted(df.candidate_uid.unique()),
        "parties": sorted(
            {map_prefix_party[u.split("-")[0]] for u in df.party_uid.unique()}
        ),
        "coalitions": sorted(
            {
                map_prefix_coalition[u.split("-")[0]]
                for u in df.coalition_uid.unique()
                if u != "000-ALONE"
            }
        ),
        "seats": sorted({generate_slug(f"{s}, {STATE}") for s in df.seat.unique()}),
        "seat_names": sorted({f"{s}, {STATE}" for s in df.seat.unique()}),
    }
    print(
        f"\nJohor SE-16 scope: {len(scope['candidates'])} candidates, "
        f"{len(scope['parties'])} parties, {len(scope['coalitions'])} coalitions, "
        f"{len(scope['seats'])} seats"
    )
    return scope


def upload_elections_se16(client, bucket):
    """Upload the internal election files touched by SE-16, and purge their cache.

    make_elections_jsons() rewrites all 459 internal election files, but SE-16 only
    changes its own — plus all.json, the consolidated file that contains it. The rest
    are byte-identical to what is already in R2.
    """
    keys = ["elections/all.json", f"elections/{STATE}/{ELECTION}.json"]
    files_to_upload = [(f"{PATH_LOCAL_INTERNAL}{k}", k) for k in keys]
    print(f"\nUploading {len(files_to_upload):,.0f} election files to R2")
    upload_bulk(client, bucket, files_to_upload, max_workers=120)
    print(f"\nPurging {len(keys):,.0f} election URL(s) from cache")
    purge_cf_cache(keys, INTERNAL_BASE_URL)


def se16_result_keys(seats):
    """R2 keys for the results files of the SE-16 polling date — one per seat.

    Scoped by date rather than by candidate uid: several SE-16 candidates have
    contested before, so a uid-based scope would drag in their past contests too.
    """
    return [f"results/{s}/{DATE}.json" for s in seats]


def upload_results_se16(client, bucket, keys):
    """Upload the SE-16 results files to the internal bucket."""
    files_to_upload = sorted([(f"{PATH_LOCAL_INTERNAL}{k}", k) for k in keys])
    print(f"\nUploading {len(files_to_upload):,.0f} results files to R2")
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def purge_results_se16(keys):
    """Purge the Cloudflare cache for the SE-16 results files."""
    print(f"\nPurging {len(keys):,.0f} results URL(s) from cache")
    purge_cf_cache(keys, INTERNAL_BASE_URL)


def duplicate_results_se16(client, source_bucket, dest_bucket, keys):
    """Copy the SE-16 results files from the internal bucket into v1/ on the API bucket."""
    for key in keys:
        client.copy_object(
            CopySource={"Bucket": source_bucket, "Key": key},
            Bucket=dest_bucket,
            Key=f"v1/{key}",
        )
    print(f"\nCopied {len(keys):,.0f} results file(s) to {dest_bucket}/v1/")


def upload_api_parties_se16(client, bucket, parties, coalitions):
    """Upload the API party/coalition files touched by SE-16.

    SE-16 rows only land in the Johor x dun slice, so only those files change —
    plus the dropdown, which gains any party contesting for the first time.
    """
    files = (
        [f"{PATH_LOCAL_API_PARTIES}dropdown.json"]
        + [f"{PATH_LOCAL_API_PARTIES}{u}/{STATE}-dun.json" for u in parties]
        + [f"{PATH_LOCAL_API_COALITIONS}{u}/{STATE}-dun.json" for u in coalitions]
    )
    files = [f for f in files if os.path.exists(f)]
    print(f"\nUploading {len(files):,.0f} API party/coalition files to R2")
    files_to_upload = sorted([(f, f.replace("api.electiondata.my/", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def upload_api_seats_se16(client, bucket, seats):
    """Upload the API seat files for the 56 Johor duns, plus the dropdown."""
    files = [f"{PATH_API_SEATS}dropdown.json"]
    for slug in seats:
        files += [f"{PATH_API_SEATS}{slug}.json", f"{PATH_API_SEATS}{slug}-lineage.json"]
    files = [f for f in files if os.path.exists(f)]
    print(f"\nUploading {len(files):,.0f} API seat files to R2")
    files_to_upload = sorted([(f, f.replace("api.electiondata.my/", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def upload_api_election_se16(client, bucket):
    """Upload the API election files for Johor SE-16 only."""
    files = [
        f"{PATH_API_ELECTIONS}{STATE}/{ELECTION}-{c}.json"
        for c in ["by_party", "by_seat", "stats"]
    ]
    files = [f for f in files if os.path.exists(f)]
    print(f"\nUploading {len(files):,.0f} API election files to R2")
    files_to_upload = sorted([(f, f.replace("api.electiondata.my/", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


if __name__ == "__main__":
    START = datetime.now()
    print(f'\nStart: {START.strftime("%Y-%m-%d %H:%M:%S")}')

    CLIENT = get_r2_client()
    BUCKET_INTERNAL = os.getenv("R2_BUCKET_INTERNAL")
    BUCKET_API = os.getenv("R2_BUCKET_API")

    SCOPE = get_se16_scope()

    # ----- internal: candidates, parties, elections, seats/current (full) -----
    make_candidates_df()
    make_candidates_jsons()
    upload_candidates_jsons(CLIENT, BUCKET_INTERNAL)
    purge_candidates_cache()

    make_parties_df()
    make_parties_jsons()
    upload_parties_jsons(CLIENT, BUCKET_INTERNAL)
    purge_parties_cache()

    make_election_stats()
    make_elections_by_seat()
    make_elections_jsons()
    upload_elections_se16(CLIENT, BUCKET_INTERNAL)

    make_seats()
    upload_seats_jsons(CLIENT, BUCKET_INTERNAL)
    purge_seats_cache()

    # ----- internal: results for the SE-16 polling date only -----
    # make_results regenerates every contest these candidates have ever stood in (some
    # have contested before), which is idempotent; only the SE-16 date is then shipped
    RESULT_KEYS = se16_result_keys(SCOPE["seat_names"])
    make_results(SCOPE["candidates"])
    upload_results_se16(CLIENT, BUCKET_INTERNAL, RESULT_KEYS)
    purge_results_se16(RESULT_KEYS)

    # ----- api: Johor SE-16 only -----
    make_api_candidates_jsons(SCOPE["candidates"])
    upload_api_candidates_jsons(CLIENT, BUCKET_API, SCOPE["candidates"])

    make_api_parties_jsons()
    upload_api_parties_se16(CLIENT, BUCKET_API, SCOPE["parties"], SCOPE["coalitions"])

    make_seats_api()
    upload_api_seats_se16(CLIENT, BUCKET_API, SCOPE["seats"])

    upload_api_election_se16(CLIENT, BUCKET_API)
    sync_dropdown_json_to_api(CLIENT, BUCKET_API)

    # the 56 SE-16 results files, copied from the internal bucket into v1/
    duplicate_results_se16(CLIENT, BUCKET_INTERNAL, BUCKET_API, RESULT_KEYS)

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
