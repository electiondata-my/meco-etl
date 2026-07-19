"""
Module: helper_tests.py

Shared helpers for the public API test suite in tests_api/.
It:
- Loads the EDMY_TEST bearer token from the repo-root .env
- Provides fetch_json() — an authenticated GET with retries and backoff
- Provides run_checks() — executes a list of checks over a thread pool
- Provides write_summary() — persists a machine-readable summary JSON per test

Inputs:
- .env (EDMY_TEST bearer token)

Outputs:
- tests_api/output/{test}.json summary file per test run
"""

import argparse
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

BASE = os.getenv("API_TEST_BASE", "https://api.electiondata.my/v1")
TOKEN = os.getenv("EDMY_TEST")
WORKERS = int(os.getenv("API_TEST_WORKERS", "30"))
RETRIES = 3
MAX_FAILURES_SAVED = 100
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

# Cloudflare blocks the default Python-urllib User-Agent (error 1010),
# so requests must present a curl-style User-Agent.
HEADERS = {"Authorization": f"Bearer {TOKEN}", "User-Agent": "curl/8.7.1"}


def fetch_json(path, params=None):
    """GET {BASE}{path} with auth and retries.

    Returns (status, data, error) where exactly one of data/error is None.
    Retries transient failures (429/5xx/network) with exponential backoff.
    """
    url = f"{BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)
    last_err = None
    for attempt in range(RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                status, body = resp.status, resp.read()
            break
        except urllib.error.HTTPError as e:
            status, body = e.code, e.read()
            if status in (429, 500, 502, 503, 504) and attempt < RETRIES - 1:
                time.sleep(2**attempt)
                continue
            break
        except Exception as e:  # network-level failure
            last_err = repr(e)
            if attempt < RETRIES - 1:
                time.sleep(2**attempt)
                continue
            return None, None, f"network: {last_err}"

    if status != 200:
        snippet = body[:200].decode(errors="replace").strip()
        return status, None, f"HTTP {status}: {snippet}"
    try:
        return status, json.loads(body), None
    except json.JSONDecodeError as e:
        return status, None, f"invalid JSON: {e}"


def run_checks(checks, label):
    """Run checks over a thread pool.

    checks: list of (id, fn) where fn() returns None on pass or an error string.
    Returns (total, failures, duration_s) with failures as [{"id", "error"}].
    """
    failures, done = [], 0
    start = time.time()
    print(f"[{label}] running {len(checks):,} checks with {WORKERS} workers", flush=True)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fn): check_id for check_id, fn in checks}
        for fut in as_completed(futures):
            check_id = futures[fut]
            done += 1
            try:
                err = fut.result()
            except Exception as e:  # a buggy check should surface, not crash the run
                err = f"check raised: {e!r}"
            if err:
                failures.append({"id": check_id, "error": err})
                print(f"[{label}] FAIL {check_id}: {err}", flush=True)
            if done % 2000 == 0:
                rate = done / (time.time() - start)
                print(
                    f"[{label}] {done:,}/{len(checks):,} done, "
                    f"{len(failures)} failures, {rate:.0f} req/s",
                    flush=True,
                )
    return len(checks), failures, time.time() - start


def write_summary(test, total, failures, duration_s):
    """Write tests_api/output/{test}.json and return the summary dict."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    summary = {
        "test": test,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total": total,
        "passed": total - len(failures),
        "failed": len(failures),
        "duration_s": round(duration_s, 1),
        "failures": failures[:MAX_FAILURES_SAVED],
        "failures_truncated": len(failures) > MAX_FAILURES_SAVED,
    }
    with open(OUTPUT_DIR / f"{test}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    status = "PASS" if not failures else "FAIL"
    print(
        f"[{test}] {status}: {summary['passed']:,}/{total:,} passed "
        f"in {duration_s:,.0f}s",
        flush=True,
    )
    return summary


def sample_arg():
    """Parse an optional --sample N CLI argument for quick partial runs."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=None, help="test a random sample of N items")
    return parser.parse_args().sample


def maybe_sample(items, n):
    """Return a random sample of n items, or all items if n is None/larger."""
    if n is None or n >= len(items):
        return items
    return random.sample(items, n)
