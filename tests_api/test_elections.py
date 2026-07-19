"""
Module: test_elections.py

Tests the /v1/elections endpoints of the public API exhaustively.
It:
- Fetches /elections/dropdown and validates its structure
- Fetches /elections/{by_party,by_seat,stats}?state=&election= for every
  unique (state, election) pair in the dropdown
- Validates that each view returns a non-empty list under its expected key

Inputs:
- .env (EDMY_TEST bearer token, via helper_tests)

Outputs:
- tests_api/output/elections.json summary file
"""

import sys

from helper_tests import fetch_json, maybe_sample, run_checks, sample_arg, write_summary

VIEWS = ("by_party", "by_seat", "stats")


def check_view(state, election, view):
    """Validate one election view for one (state, election) pair."""
    _, data, err = fetch_json(f"/elections/{view}", {"state": state, "election": election})
    if err:
        return err
    rows = data.get(view)
    if not isinstance(rows, list) or not rows:
        return f"missing/empty {view}"
    return None


def main(sample=None):
    _, dropdown, err = fetch_json("/elections/dropdown")
    if err or not dropdown.get("elections"):
        return write_summary("elections", 1, [{"id": "dropdown", "error": err or "empty dropdown"}], 0)

    pairs = sorted({(e["state"], e["election"]) for e in dropdown["elections"]})
    pairs = maybe_sample(pairs, sample)
    checks = [
        (f"{state}/{election}-{view}", lambda s=state, e=election, v=view: check_view(s, e, v))
        for state, election in pairs
        for view in VIEWS
    ]
    total, failures, duration = run_checks(checks, "elections")
    return write_summary("elections", total + 1, failures, duration)  # +1 for the dropdown


if __name__ == "__main__":
    sys.exit(1 if main(sample_arg())["failed"] else 0)
