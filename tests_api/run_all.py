"""
Module: run_all.py

Runs the full public API test suite and writes one consolidated summary.
It:
- Runs the candidates, seats, elections, results, parties and byelections tests
- Writes a consolidated summary JSON for downstream reporting (e.g. Telegram)
- Prints a compact human-readable report and exits non-zero on any failure

Inputs:
- .env (EDMY_TEST bearer token, via helper_tests)

Outputs:
- tests_api/output/summary.json consolidated summary file

The summary.json schema (for the Telegram bot):
{
  "timestamp": "2026-07-19T09:00:00+00:00",
  "ok": true,
  "total": 30245, "passed": 30245, "failed": 0, "duration_s": 182.4,
  "report": "compact multi-line text, ready to send as a message",
  "suites": [ {per-test summary, see helper_tests.write_summary}, ... ]
}
"""

import json
import sys
from datetime import datetime, timezone

import test_byelections
import test_candidates
import test_elections
import test_parties
import test_results
import test_seats
from helper_tests import OUTPUT_DIR, sample_arg

SUITES = [
    test_candidates,
    test_seats,
    test_elections,
    test_results,
    test_parties,
    test_byelections,
]


def main(sample=None):
    """Run every API test suite and write a combined summary."""
    started = datetime.now(timezone.utc)
    summaries = [suite.main(sample) for suite in SUITES]

    total = sum(s["total"] for s in summaries)
    failed = sum(s["failed"] for s in summaries)
    duration = sum(s["duration_s"] for s in summaries)

    lines = [f"API tests {started.strftime('%Y-%m-%d %H:%M')} UTC"]
    for s in summaries:
        icon = "✅" if not s["failed"] else "❌"
        line = f"{icon} {s['test']}: {s['passed']:,}/{s['total']:,}"
        if s["failed"]:
            worst = "; ".join(f"{f['id']}: {f['error']}" for f in s["failures"][:3])
            line += f" — {worst}"
        lines.append(line)
    lines.append(f"Total: {total - failed:,}/{total:,} in {duration:,.0f}s")
    report = "\n".join(lines)

    summary = {
        "timestamp": started.isoformat(timespec="seconds"),
        "ok": failed == 0,
        "total": total,
        "passed": total - failed,
        "failed": failed,
        "duration_s": round(duration, 1),
        "report": report,
        "suites": summaries,
    }
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_DIR / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{report}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sample_arg()))
