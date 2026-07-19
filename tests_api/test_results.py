"""
Module: test_results.py

Tests the /v1/results endpoint of the public API exhaustively.
It:
- Enumerates every (seat, state, date) contest from the local ETL output tree
  internal.electiondata.my/results/ (the source of what was uploaded to R2)
- Fetches /results?seat=&state=&date= for every contest
- Validates that each response has a non-empty ballot and a stats entry

Inputs:
- .env (EDMY_TEST bearer token, via helper_tests)
- internal.electiondata.my/results/{seat}/{date}.json (enumeration only)

Outputs:
- tests_api/output/results.json summary file
"""

import sys

from helper_tests import ROOT, fetch_json, maybe_sample, run_checks, sample_arg, write_summary

RESULTS_DIR = ROOT / "internal.electiondata.my" / "results"


def enumerate_contests():
    """Yield (seat, state, date) for every results file in the local ETL tree."""
    contests = []
    for seat_dir in sorted(RESULTS_DIR.iterdir()):
        if not seat_dir.is_dir():
            continue
        seat, _, state = seat_dir.name.rpartition(", ")
        for f in sorted(seat_dir.glob("*.json")):
            contests.append((seat, state, f.stem))
    return contests


def check_contest(seat, state, date):
    """Validate one contest's results file."""
    _, data, err = fetch_json("/results", {"seat": seat, "state": state, "date": date})
    if err:
        return err
    ballot, stats = data.get("ballot"), data.get("stats")
    if not isinstance(ballot, list) or not ballot:
        return "missing/empty ballot"
    if not isinstance(stats, list) or not stats:
        return "missing/empty stats"
    return None


def main(sample=None):
    if not RESULTS_DIR.is_dir():
        return write_summary(
            "results", 1,
            [{"id": "enumeration", "error": f"{RESULTS_DIR} not found — run api_results.py first"}],
            0,
        )
    contests = maybe_sample(enumerate_contests(), sample)
    checks = [
        (f"{seat}, {state}/{date}", lambda s=seat, st=state, d=date: check_contest(s, st, d))
        for seat, state, date in contests
    ]
    total, failures, duration = run_checks(checks, "results")
    return write_summary("results", total, failures, duration)


if __name__ == "__main__":
    sys.exit(1 if main(sample_arg())["failed"] else 0)
