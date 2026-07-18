"""Generate seat-result card PNGs in the electiondata.my modal format.

Examples
--------
    # Completed election (validate the pipeline against known-final data)
    uv run python gen_seat_cards.py --election SE-15 --state Johor

    # Election-night use: re-run as consol_ballots/consol_stats refresh
    uv run python gen_seat_cards.py --election SE-16 --state Johor
    uv run python gen_seat_cards.py --election SE-16 --state Johor --watch 60

    # Surgically (re)generate a single seat's card
    uv run python gen_seat_cards.py --election SE-16 --state Johor --seat "Bukit Batu"
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from cards.render import render_cards
from cards.seatcard_data import build_seat_cards


def run_once(election: str, state: str, out: Path, seat: str | None = None) -> int:
    cards = build_seat_cards(election, state)
    if seat:
        needle = seat.lower()
        cards = [c for c in cards if needle in c["seat"].lower() or needle in c["slug"]]
        if not cards:
            raise SystemExit(f"No seat matching '{seat}' in {election} / {state}")
    written = render_cards(cards, out / election)
    print(f"Rendered {len(written)} card(s) -> {out / election}")
    return len(written)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--election", required=True, help="Election code, e.g. SE-15, SE-16")
    ap.add_argument("--state", required=True, help="State name, e.g. Johor")
    ap.add_argument(
        "--seat",
        default=None,
        help="Render only seats matching this (seat name or slug substring, "
        "e.g. 'Bukit Batu' or 'n51-bukit-batu-johor'). Omit for all seats.",
    )
    ap.add_argument("--out", default="output/cards", help="Output root directory")
    ap.add_argument(
        "--watch",
        type=int,
        default=0,
        metavar="SECONDS",
        help="Re-render every N seconds (election-night mode). 0 = run once.",
    )
    args = ap.parse_args()

    out = Path(args.out)
    if args.watch <= 0:
        run_once(args.election, args.state, out, args.seat)
        return

    print(f"Watch mode: re-rendering {args.election}/{args.state} every {args.watch}s "
          f"(Ctrl-C to stop).")
    while True:
        run_once(args.election, args.state, out, args.seat)
        time.sleep(args.watch)


if __name__ == "__main__":
    main()
