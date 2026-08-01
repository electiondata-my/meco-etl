"""
Module: api_election_night.py

Runs the full pipeline for the internal site, but restricts the public API upload to the Negeri Sembilan SE-16 election only.
It:
- Regenerates and uploads the internal candidates, parties and seats/current files (dropdown + all.json)
- Regenerates the internal election files, but uploads only elections/all.json and elections/Negeri Sembilan/SE-16.json
- Regenerates and uploads the internal results files for the SE-16 polling date only
- Derives the SE-16 contestants (candidates, parties, coalitions, seats) from consol_ballots
- Uploads to the API bucket only: the SE-16 candidates, their parties/coalitions (Negeri Sembilan-dun), the 36 Negeri Sembilan duns, the Negeri Sembilan SE-16 election, and the 36 SE-16 results files
- Regenerates and uploads the lake headline files touched by SE-16 (headline_{ballots,stats} and their state_nsn slices)
- Refreshes data_as_of in the catalogue index for those datasets, rebuilds their catalogue JSONs, and uploads both

Inputs:
- {PATH_RESULTS_HEADLINE}consol_ballots.parquet
- {PATH_RESULTS_HEADLINE}consol_stats.parquet
- {PATH_RESULTS_HEADLINE}lookup_party.parquet
- {PATH_RESULTS_HEADLINE}lookup_coalition.parquet
- template-catalogue/headline-{ballots,stats}.json
- internal.electiondata.my/catalogue/index.json

Outputs:
- internal.electiondata.my/{candidates,parties,seats/current}/{dropdown,all}.json uploaded to R2
- internal.electiondata.my/elections/{all.json,Negeri Sembilan/SE-16.json} uploaded to R2
- internal.electiondata.my/results/{seat}/2026-08-01.json uploaded to R2
- api.electiondata.my/v1/candidates/{uid}.json uploaded to R2 (SE-16 candidates only)
- api.electiondata.my/v1/{parties,coalitions}/{uid}/Negeri Sembilan-dun.json uploaded to R2 (SE-16 parties only)
- api.electiondata.my/v1/seats/current/{slug}.json uploaded to R2 (36 Negeri Sembilan duns only)
- api.electiondata.my/v1/elections/Negeri Sembilan/SE-16-*.json uploaded to R2
- api.electiondata.my/v1/results/{seat}/2026-08-01.json copied to R2 (36 files)
- lake.electiondata.my/results_headline/headline_{ballots,stats}[_state_nsn].{csv,parquet,xlsx} uploaded to R2
- internal.electiondata.my/catalogue/{index,headline-*}.json uploaded to R2
"""

import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv

from helper import (
    generate_slug,
    get_r2_client,
    upload_bulk,
    purge_cf_cache,
    write_csv_parquet_excel,
)

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
from api_results import (
    make_results,
    purge_results_cache,
    PATH_LOCAL_INTERNAL,
    INTERNAL_BASE_URL,
)
from api_seats_current import (
    make_seats,
    upload_seats_jsons,
    purge_seats_cache,
    make_seats_api,
    PATH_LOCAL_API as PATH_API_SEATS,
)
from api_catalogue_results_headline import (
    _build_catalogue,
    TEMPLATE_DIR,
    purge_catalogue_results_headline_cache,
)

load_dotenv()
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")
PATH_LOCAL_LAKE = "lake.electiondata.my/"

DATE = "2026-08-01"
ELECTION = "SE-16"
STATE = "Negeri Sembilan"
STATE_CODE = "nsn"

# the catalogue datasets whose underlying data changes with this election
HEADLINE_IDS = [
    "headline-ballots",
    "headline-stats",
    f"headline-ballots-state-{STATE_CODE}",
    f"headline-stats-state-{STATE_CODE}",
]


def get_se16_scope():
    """Resolve everything contesting Negeri Sembilan SE-16 from the consolidated ballots.

    Party and coalition uids are normalised the same way api_parties.py does it (the
    numeric prefix is the stable key), because that is the uid the API folders use.

    Returns:
        dict with keys: candidates (uids), parties (uids), coalitions (uids),
        seats (slugs, as used by the seats API), seat_names ("N.01 Chennah, Negeri Sembilan",
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
        f"\n{STATE} {ELECTION} scope: {len(scope['candidates'])} candidates, "
        f"{len(scope['parties'])} parties, {len(scope['coalitions'])} coalitions, "
        f"{len(scope['seats'])} seats"
    )
    return scope


def se16_election_keys():
    """R2 keys for the internal election files touched by SE-16.

    make_elections_jsons() rewrites all 459 internal election files, but SE-16 only
    changes its own — plus all.json, the consolidated file that contains it. The rest
    are byte-identical to what is already in R2.
    """
    return ["elections/all.json", f"elections/{STATE}/{ELECTION}.json"]


def upload_elections_se16(client, bucket, keys):
    """Upload the internal election files touched by SE-16."""
    files_to_upload = [(f"{PATH_LOCAL_INTERNAL}{k}", k) for k in keys]
    print(f"\nUploading {len(files_to_upload):,.0f} election files to R2")
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def purge_elections_se16(keys):
    """Purge the Cloudflare cache for the internal SE-16 election files."""
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


def purge_results_se16():
    """Purge the Cloudflare cache for the whole internal results prefix.

    Purged by prefix rather than by key: results keys carry the raw seat name
    ("results/N.01 Chennah, Negeri Sembilan/..."), so a URL purge would send unencoded spaces
    and commas and miss the percent-encoded key Cloudflare actually caches.
    """
    purge_results_cache()


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

    SE-16 rows only land in the Negeri Sembilan x dun slice, so only those files change —
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
    """Upload the API seat files for the 36 Negeri Sembilan duns, plus the dropdown."""
    files = [f"{PATH_API_SEATS}dropdown.json"]
    for slug in seats:
        files += [f"{PATH_API_SEATS}{slug}.json", f"{PATH_API_SEATS}{slug}-lineage.json"]
    files = [f for f in files if os.path.exists(f)]
    print(f"\nUploading {len(files):,.0f} API seat files to R2")
    files_to_upload = sorted([(f, f.replace("api.electiondata.my/", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def upload_api_election_se16(client, bucket):
    """Upload the API election files for Negeri Sembilan SE-16 only."""
    files = [
        f"{PATH_API_ELECTIONS}{STATE}/{ELECTION}-{c}.json"
        for c in ["by_party", "by_seat", "stats"]
    ]
    files = [f for f in files if os.path.exists(f)]
    print(f"\nUploading {len(files):,.0f} API election files to R2")
    files_to_upload = sorted([(f, f.replace("api.electiondata.my/", "")) for f in files])
    upload_bulk(client, bucket, files_to_upload, max_workers=120)


def make_lake_headline_se16():
    """Regenerate the lake headline files touched by SE-16.

    lake_results_headline.py rewrites every slice; SE-16 rows only land in the
    full files and the Negeri Sembilan state slice, so only those are rebuilt.
    """
    for v in ["ballots", "stats"]:
        df = pd.read_parquet(f"{PATH_RESULTS_HEADLINE}consol_{v}.parquet")
        write_csv_parquet_excel(f"{PATH_LOCAL_LAKE}results_headline/headline_{v}", df)
        write_csv_parquet_excel(
            f"{PATH_LOCAL_LAKE}results_headline/headline_{v}_state_{STATE_CODE}",
            df[(df.state == STATE) & (df.election.str.startswith("SE-"))],
        )


def upload_lake_headline_se16(client, bucket):
    """Upload the SE-16 lake headline files to R2 meco-lake."""
    for ext, content_type in [
        ("csv", "text/csv"),
        ("parquet", "application/octet-stream"),
        ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    ]:
        files = [
            f"{PATH_LOCAL_LAKE}results_headline/{i.replace('-', '_')}.{ext}"
            for i in HEADLINE_IDS
        ]
        print(f"\nUploading {len(files):,.0f} {ext.upper()} files to R2")
        files_to_upload = [(f, f.replace(PATH_LOCAL_LAKE, "")) for f in files]
        upload_bulk(client, bucket, files_to_upload, content_type=content_type)


def make_catalogue_headline_se16():
    """Refresh data_as_of in the catalogue index and rebuild the SE-16 catalogue JSONs."""
    index_path = Path(f"{PATH_LOCAL_INTERNAL}catalogue/index.json")
    index = json.loads(index_path.read_text(encoding="utf-8"))
    entries = [
        e for e in index["data"]["Results"]["Constituency-Level"] if e["id"] in HEADLINE_IDS
    ]
    for entry in entries:
        entry["data_as_of"] = DATE
    index_path.write_text(
        json.dumps(index, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )
    print(f"\nSet data_as_of = {DATE} for {len(entries)} catalogue index entries")

    templates = {
        "ballots": json.loads((TEMPLATE_DIR / "headline-ballots.json").read_text()),
        "stats": json.loads((TEMPLATE_DIR / "headline-stats.json").read_text()),
    }
    out_dir = Path(PATH_LOCAL_INTERNAL) / "catalogue" / "results_headline"
    out_dir.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        template_key = "stats" if "stats" in entry["id"] else "ballots"
        cat = _build_catalogue(templates[template_key], entry)
        out_path = out_dir / f"{entry['id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(cat, f, ensure_ascii=False)
        print(f"Written: {out_path}")


def upload_catalogue_headline_se16(client, bucket):
    """Upload the SE-16 catalogue JSONs and the index to the internal bucket."""
    files_to_upload = sorted(
        [
            (f"{PATH_LOCAL_INTERNAL}catalogue/results_headline/{i}.json", f"catalogue/{i}.json")
            for i in HEADLINE_IDS
        ]
    ) + [(f"{PATH_LOCAL_INTERNAL}catalogue/index.json", "catalogue/index.json")]
    print(f"\nUploading {len(files_to_upload):,.0f} catalogue files to R2")
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

    ELECTION_KEYS = se16_election_keys()
    make_election_stats()
    make_elections_by_seat()
    make_elections_jsons()
    upload_elections_se16(CLIENT, BUCKET_INTERNAL, ELECTION_KEYS)
    purge_elections_se16(ELECTION_KEYS)

    make_seats()
    upload_seats_jsons(CLIENT, BUCKET_INTERNAL)
    purge_seats_cache()

    # ----- internal: results for the SE-16 polling date only -----
    # make_results regenerates every contest these candidates have ever stood in (some
    # have contested before), which is idempotent; only the SE-16 date is then shipped
    RESULT_KEYS = se16_result_keys(SCOPE["seat_names"])
    make_results(SCOPE["candidates"])
    upload_results_se16(CLIENT, BUCKET_INTERNAL, RESULT_KEYS)
    purge_results_se16()

    # ----- api: Negeri Sembilan SE-16 only -----
    make_api_candidates_jsons(SCOPE["candidates"])
    upload_api_candidates_jsons(CLIENT, BUCKET_API, SCOPE["candidates"])

    make_api_parties_jsons()
    upload_api_parties_se16(CLIENT, BUCKET_API, SCOPE["parties"], SCOPE["coalitions"])

    make_seats_api()
    upload_api_seats_se16(CLIENT, BUCKET_API, SCOPE["seats"])

    upload_api_election_se16(CLIENT, BUCKET_API)
    sync_dropdown_json_to_api(CLIENT, BUCKET_API)

    # the 36 SE-16 results files, copied from the internal bucket into v1/
    duplicate_results_se16(CLIENT, BUCKET_INTERNAL, BUCKET_API, RESULT_KEYS)

    # ----- lake + catalogue: headline files touched by SE-16 only -----
    BUCKET_LAKE = os.getenv("R2_BUCKET_LAKE")
    make_lake_headline_se16()
    upload_lake_headline_se16(CLIENT, BUCKET_LAKE)

    make_catalogue_headline_se16()
    upload_catalogue_headline_se16(CLIENT, BUCKET_INTERNAL)
    purge_catalogue_results_headline_cache()

    print(f'\nEnd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"\nDuration: {datetime.now() - START}\n")
