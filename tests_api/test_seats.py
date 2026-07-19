"""
Module: test_seats.py

Tests the /v1/seats endpoints of the public API exhaustively.
It:
- Fetches /seats/dropdown and validates its structure
- Fetches /seats/results?slug= for every slug in the dropdown
- Fetches /seats/results?slug=&lineage=true for every slug
- Validates that both variants return non-empty results with required fields

Inputs:
- .env (EDMY_TEST bearer token, via helper_tests)

Outputs:
- tests_api/output/seats.json summary file
"""

import sys

from helper_tests import fetch_json, maybe_sample, run_checks, sample_arg, write_summary

REQUIRED_FIELDS = ("date", "seat", "state")


def check_seat(slug):
    """Validate one seat's headline results."""
    _, data, err = fetch_json("/seats/results", {"slug": slug})
    if err:
        return err
    results = data.get("results")
    if not isinstance(results, list) or not results:
        return "missing/empty results"
    for r in results:
        missing = [k for k in REQUIRED_FIELDS if not r.get(k)]
        if missing:
            return f"contest {r.get('date')} missing: {missing}"
    return None


def check_seat_lineage(slug):
    """Validate one seat's lineage results."""
    _, data, err = fetch_json("/seats/results", {"slug": slug, "lineage": "true"})
    if err:
        return err
    results = data.get("results")
    if not isinstance(results, list) or not results:
        return "missing/empty lineage results"
    return None


def main(sample=None):
    _, dropdown, err = fetch_json("/seats/dropdown")
    if err or not dropdown.get("seats"):
        return write_summary("seats", 1, [{"id": "dropdown", "error": err or "empty dropdown"}], 0)

    slugs = [s["slug"] for s in maybe_sample(dropdown["seats"], sample)]
    checks = [(slug, lambda s=slug: check_seat(s)) for slug in slugs]
    checks += [(f"{slug}-lineage", lambda s=slug: check_seat_lineage(s)) for slug in slugs]
    total, failures, duration = run_checks(checks, "seats")
    return write_summary("seats", total + 1, failures, duration)  # +1 for the dropdown


if __name__ == "__main__":
    sys.exit(1 if main(sample_arg())["failed"] else 0)
