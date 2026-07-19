"""
Module: test_parties.py

Tests the /v1/parties endpoints of the public API exhaustively.
It:
- Fetches /parties/dropdown and validates its structure
- Derives the unique canonical uids (maps_to) for parties and coalitions
- Fetches /parties/results for every uid x state x election_type combination
  the worker accepts (18 states for parlimen, 13 for dun — 31 per uid)
- Validates that each response contains a results list

Inputs:
- .env (EDMY_TEST bearer token, via helper_tests)

Outputs:
- tests_api/output/parties.json summary file
"""

import sys

from helper_tests import fetch_json, maybe_sample, run_checks, sample_arg, write_summary

# Mirrors VALID_STATES and the DUN exclusions in meco-back/workers/api/worker.js
PARLIMEN_STATES = [
    "Malaysia", "Semenanjung",
    "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan",
    "Pahang", "Perak", "Perlis", "Pulau Pinang",
    "Sabah", "Sarawak", "Selangor", "Terengganu",
    "W.P. Kuala Lumpur", "W.P. Labuan", "W.P. Putrajaya",
]
DUN_STATES = [
    s for s in PARLIMEN_STATES
    if s not in ("Malaysia", "Semenanjung", "W.P. Kuala Lumpur", "W.P. Labuan", "W.P. Putrajaya")
]


def check_combo(party_type, uid, state, election_type):
    """Validate one party/coalition results file."""
    _, data, err = fetch_json(
        "/parties/results",
        {"type": party_type, "uid": uid, "state": state, "election_type": election_type},
    )
    if err:
        return err
    if not isinstance(data.get("results"), list):
        return "missing results list"
    return None


def main(sample=None):
    _, dropdown, err = fetch_json("/parties/dropdown")
    if err or not dropdown.get("data"):
        return write_summary("parties", 1, [{"id": "dropdown", "error": err or "empty dropdown"}], 0)

    uids = sorted({(d["type"], d["maps_to"]) for d in dropdown["data"]})
    combos = [
        (party_type, uid, state, election_type)
        for party_type, uid in uids
        for election_type, states in (("parlimen", PARLIMEN_STATES), ("dun", DUN_STATES))
        for state in states
    ]
    combos = maybe_sample(combos, sample)
    checks = [
        (f"{t}/{u}/{s}-{e}", lambda t=t, u=u, s=s, e=e: check_combo(t, u, s, e))
        for t, u, s, e in combos
    ]
    total, failures, duration = run_checks(checks, "parties")
    return write_summary("parties", total + 1, failures, duration)  # +1 for the dropdown


if __name__ == "__main__":
    sys.exit(1 if main(sample_arg())["failed"] else 0)
