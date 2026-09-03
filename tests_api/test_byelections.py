"""
Module: test_byelections.py

Tests the /v1/byelections endpoint of the public API.
It:
- Fetches /byelections (a single consolidated file)
- Validates that the list is non-empty and every entry has seat, state and date

Inputs:
- .env (EDMY_TEST bearer token, via helper_tests)

Outputs:
- tests_api/output/byelections.json summary file
"""

import sys

from helper_tests import fetch_json, run_checks, sample_arg, write_summary

REQUIRED_FIELDS = ("seat", "state", "date")


def check_byelections():
    """Validate the consolidated by-elections file."""
    _, data, err = fetch_json("/byelections")
    if err:
        return err
    rows = data.get("byelections")
    if not isinstance(rows, list) or not rows:
        return "missing/empty byelections"
    for i, r in enumerate(rows):
        missing = [k for k in REQUIRED_FIELDS if not r.get(k)]
        if missing:
            return f"entry {i} ({r.get('seat')}) missing: {missing}"
    return None


def main(sample=None):  # pylint: disable=unused-argument
    """Run the byelections API checks and write a summary.

    Takes `sample` for a uniform suite interface; this suite is a single
    check, so there is nothing to sample.
    """
    checks = [("byelections", check_byelections)]
    total, failures, duration = run_checks(checks, "byelections")
    return write_summary("byelections", total, failures, duration)


if __name__ == "__main__":
    sys.exit(1 if main(sample_arg())["failed"] else 0)
