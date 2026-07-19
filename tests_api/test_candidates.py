"""
Module: test_candidates.py

Tests the /v1/candidates endpoints of the public API exhaustively.
It:
- Fetches /candidates/dropdown and validates its structure
- Fetches /candidates?uid= for every uid in the dropdown
- Validates each response's results against the dropdown's c/w/l counts

Inputs:
- .env (EDMY_TEST bearer token, via helper_tests)

Outputs:
- tests_api/output/candidates.json summary file
"""

import sys

from helper_tests import fetch_json, maybe_sample, run_checks, sample_arg, write_summary

REQUIRED_FIELDS = ("date", "seat", "state", "result")


def check_candidate(cand):
    """Validate one candidate's API response against its dropdown entry."""
    _, data, err = fetch_json("/candidates", {"uid": cand["uid"]})
    if err:
        return err
    results = data.get("results")
    if not isinstance(results, list) or not results:
        return "missing/empty results"

    problems = []
    if len(results) != cand["c"]:
        problems.append(f"contests {len(results)} != dropdown c {cand['c']}")
    won = sum(1 for r in results if r.get("result") and "won" in r["result"])
    lost = sum(1 for r in results if r.get("result") and "lost" in r["result"])
    if won != cand["w"]:
        problems.append(f"wins {won} != dropdown w {cand['w']}")
    if lost != cand["l"]:
        problems.append(f"losses {lost} != dropdown l {cand['l']}")
    if cand["name"] not in {r.get("name") for r in results}:
        problems.append(f"name {cand['name']!r} not in results")
    for r in results:
        missing = [k for k in REQUIRED_FIELDS if not r.get(k)]
        if missing:
            problems.append(f"contest {r.get('date')}/{r.get('seat')} missing: {missing}")
    return "; ".join(problems) if problems else None


def main(sample=None):
    _, dropdown, err = fetch_json("/candidates/dropdown")
    if err or not dropdown.get("candidates"):
        return write_summary("candidates", 1, [{"id": "dropdown", "error": err or "empty dropdown"}], 0)

    candidates = maybe_sample(dropdown["candidates"], sample)
    checks = [(c["uid"], lambda c=c: check_candidate(c)) for c in candidates]
    total, failures, duration = run_checks(checks, "candidates")
    return write_summary("candidates", total + 1, failures, duration)  # +1 for the dropdown


if __name__ == "__main__":
    sys.exit(1 if main(sample_arg())["failed"] else 0)
