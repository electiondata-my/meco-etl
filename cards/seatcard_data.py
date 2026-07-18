"""Build seat-result card data in the electiondata.my modal format.

One reusable adapter reads the consolidated ballot/stats tables (which already
carry every election, including the live SE-16 Johor rows) and emits a uniform
per-seat dict consumed by ``card.html.j2``. On election night, re-running after
``consol_ballots``/``consol_stats`` are refreshed regenerates the cards.
"""

from __future__ import annotations

import base64
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from helper import generate_slug  # existing slug util -> "n51-bukit-batu-johor"

load_dotenv()
PATH_RESULTS_HEADLINE = os.getenv("PATH_RESULTS_HEADLINE")

# Party/coalition flags live in the ElectionData.MY frontend repo, keyed by uid.
FRONTEND = Path("/Users/theveshtheva/git-edmy/meco-front")
PARTIES_DIR = FRONTEND / "public/static/images/parties"
COALITIONS_DIR = FRONTEND / "public/static/images/coalitions"

_logo_cache: dict[tuple[str, str], str | None] = {}


def _isna(v) -> bool:
    return v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v)


def _flag_data_uri(directory: Path, uid: str | None) -> str | None:
    """Embed a flag PNG as a base64 data URI (keyed by uid)."""
    if not uid or _isna(uid):
        return None
    key = (str(directory), uid)
    if key in _logo_cache:
        return _logo_cache[key]
    path = directory / f"{uid}.png"
    uri = None
    if path.exists():
        uri = "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode()
    _logo_cache[key] = uri
    return uri


def _party_logo(uid: str | None) -> str | None:
    return _flag_data_uri(PARTIES_DIR, uid)


def _coalition_logo(uid: str | None) -> str | None:
    return _flag_data_uri(COALITIONS_DIR, uid)


def _int_str(v) -> str:
    return "—" if _isna(v) else f"{int(round(float(v))):,}"


def _pct_text(v, null: str = "—") -> str:
    return null if _isna(v) else f"{float(v):.1f}%"


def _pct_width(v) -> str:
    return f"{min(0.0 if _isna(v) else float(v), 100.0)}%"


def _fmt_date(date_str) -> str:
    return datetime.strptime(str(date_str)[:10], "%Y-%m-%d").strftime("%-d %b %Y")


def build_seat_cards(
    election: str,
    state: str,
    ballots_path: str | Path | None = None,
    stats_path: str | Path | None = None,
) -> list[dict]:
    """Return one card dict per seat for ``election`` in ``state``."""
    ballots = pd.read_parquet(ballots_path or f"{PATH_RESULTS_HEADLINE}consol_ballots.parquet")
    stats = pd.read_parquet(stats_path or f"{PATH_RESULTS_HEADLINE}consol_stats.parquet")

    b = ballots[(ballots.election == election) & (ballots.state == state)]
    s = stats[(stats.election == election) & (stats.state == state)]
    if b.empty:
        raise ValueError(f"No ballots found for {election} / {state}")
    stats_by_seat = {row.seat: row for row in s.itertuples()}
    # Electorate relative to the mean seat size across this election+state (matches
    # meco-etl/api_results.py _voters_total_v_avg for state seats).
    avg_electorate = s.drop_duplicates("seat").voters_total.mean()

    cards: list[dict] = []
    for seat, grp in b.groupby("seat", sort=False):
        grp = grp.assign(_v=grp["votes"].fillna(0)).sort_values(
            ["_v", "ballot_order"], ascending=[False, True]
        )
        st = stats_by_seat.get(seat)

        candidates = []
        for r in grp.itertuples():
            votes = None if _isna(r.votes) else r.votes
            aligned = bool(r.coalition) and r.coalition != "ALONE"
            candidates.append(
                {
                    "name": r.name,
                    # Coalition-aligned: "COALITION (PARTY)" + coalition flag.
                    # Independent/unaligned: bare party name + party flag.
                    "party_display": f"{r.coalition} ({r.party})" if aligned else r.party,
                    "logo_uri": _coalition_logo(r.coalition_uid) if aligned else _party_logo(r.party_uid),
                    "votes_str": "0" if votes is None else f"{int(votes):,}",
                    "votes_perc_str": _pct_text(r.votes_perc),
                    "bar_width": _pct_width(r.votes_perc),
                    "is_winner": str(r.result).startswith("won"),
                }
            )

        def pct_stat(label, abs_v, perc_v):
            return {
                "label": label,
                "text": f"{_int_str(abs_v)} ({_pct_text(perc_v)})",
                "bar_width": _pct_width(perc_v),
                "show_bar": not _isna(perc_v),
            }

        def ratio_stat(label, abs_v, ratio_v):
            has = not _isna(ratio_v)
            return {
                "label": label,
                "text": f"{_int_str(abs_v)} ({ratio_v:.2f}x avg)" if has else f"{_int_str(abs_v)} (—)",
                "bar_width": f"{min((ratio_v / 2) * 100, 100)}%" if has else "0%",
                "show_bar": has,
            }

        stats_list = []
        if st is not None:
            electorate_ratio = (
                st.voters_total / avg_electorate
                if not _isna(st.voters_total) and not _isna(avg_electorate) and avg_electorate
                else None
            )
            stats_list = [
                pct_stat("Majority", st.majority, st.majority_perc),
                ratio_stat("Electorate", st.voters_total, electorate_ratio),
                pct_stat("Voter Turnout", st.ballots_issued, st.voter_turnout),
                pct_stat("Rejected Votes", st.votes_rejected, st.votes_rejected_perc),
            ]

        cards.append(
            {
                "election_display": election,
                "date_str": _fmt_date(grp["date"].iloc[0]),
                "seat": seat,
                "state": state,
                "slug": generate_slug(f"{seat} {state}"),
                "candidates": candidates,
                "stats": stats_list,
            }
        )
    return cards
