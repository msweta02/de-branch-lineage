#!/usr/bin/env python3
"""Rebuild and run the pipeline under each bug combination, and report.

Answers the questions Phase 0 has to answer before anything is built on top of
this pipeline:

* what is the headline desk_summary figure when the pipeline is healthy
* what is it with BUG A on, and what is the exact delta
* what is it with BUG B on
* how many rows, and how many NULL measures, exist at every hop

Row counts alone do not locate either bug: BUG A loses no rows at all, and BUG B
loses rows only inside a CTE. The null counts are what move. That contrast is
the reason this project exists, so the report prints both.

Each scenario is built by re-seeding the warehouse in place, so on exit the
warehouse is left in the state of the *last* scenario built -- ``both`` by
default, not ``healthy``. Re-seed before using it as a healthy baseline.

Usage::

    python tests/verify_scenarios.py
    python tests/verify_scenarios.py --only healthy bug_a
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PY = str(ROOT / ".venv" / "bin" / "python") if (ROOT / ".venv" / "bin" / "python").exists() else sys.executable
DB = ROOT / "include" / "data" / "warehouse.duckdb"

BUG_A_DATE = "2026-09-03"
BUG_B_DATE = "2026-08-31"

SCENARIOS = {
    "healthy": [],
    "bug_a": ["--bug-a"],
    "bug_b": ["--bug-b"],
    "both": ["--bug-a", "--bug-b"],
}

# (relation, measure column that goes NULL when something upstream broke)
HOPS = [
    ("ref.calendar",           None),
    ("ref.fx_rates",           "rate"),
    ("ref.book_hierarchy",     None),
    ("ref.counterparty",       None),
    ("ref.validity_window",    None),
    ("trd.trades_ingested",    None),
    ("trd.trades_deduped",     None),
    ("trd.trades_amended",     "gross_amount_local"),
    ("trd.trades_enriched",    "gross_amount_local"),
    ("trd.trades_converted",   "base_amount"),
    ("trd.trades_classified",  "base_amount"),
    ("pos.book_positions",     "gross_amount_usd"),
    ("pos.positions_adjusted", "net_amount_usd"),
    ("pos.positions_marked",   "mtm_pnl_usd"),
    ("pos.positions_accrued",  "accrued_interest_usd"),
    ("pos.positions_netted",   "mtm_pnl_usd"),
    ("rpt.pnl_daily",          "total_pnl_usd"),
    ("rpt.regulatory_extract", "total_pnl_usd"),
    ("rpt.desk_summary",       "total_pnl_usd"),
    ("rpt.recon_control",      "amount_break_usd"),
]


def build(flags: list[str]) -> None:
    subprocess.run([PY, str(ROOT / "include" / "seed" / "generate.py"), *flags],
                   check=True, capture_output=True, cwd=ROOT)
    subprocess.run([PY, str(ROOT / "include" / "runner.py")],
                   check=True, capture_output=True, cwd=ROOT)


def measure() -> dict:
    con = duckdb.connect(str(DB), read_only=True)
    q = lambda s: con.execute(s).fetchone()[0]

    out = {
        "firm_pnl_bug_a_date": q(
            f"SELECT SUM(total_pnl_usd) FROM rpt.desk_summary "
            f"WHERE business_date = DATE '{BUG_A_DATE}'"),
        "firm_pnl_bug_b_date": q(
            f"SELECT SUM(total_pnl_usd) FROM rpt.desk_summary "
            f"WHERE business_date = DATE '{BUG_B_DATE}'"),
        "firm_pnl_all_dates": q("SELECT SUM(total_pnl_usd) FROM rpt.desk_summary"),
        "accrual_all_dates": q("SELECT SUM(accrued_interest_usd) FROM rpt.desk_summary"),
        "fx_rate_rows": q("SELECT COUNT(*) FROM ref.fx_rates"),
        "null_base_amount": q(
            "SELECT COUNT(*) FROM trd.trades_classified WHERE base_amount IS NULL"),
        "residual_tier": q(
            "SELECT COUNT(*) FROM trd.trades_classified WHERE size_tier = 'TIER_5_RESIDUAL'"),
        "null_accrual": q(
            "SELECT COUNT(*) FROM pos.positions_accrued WHERE accrued_interest_usd IS NULL"),
        "accrual_basis_rows": q(
            "SELECT COUNT(*) FROM ref.calendar c JOIN raw.rate_curve o "
            "ON o.curve_date = c.calendar_date AND o.tenor = 'ON' "
            "WHERE c.calendar_date >= c.accrual_period_start "
            "  AND c.calendar_date <  c.accrual_period_end"),
        "control_breaks": q(
            "SELECT COUNT(*) FROM rpt.recon_control WHERE control_status = 'BREAK'"),
    }

    hops = []
    for rel, measure_col in HOPS:
        rows = q(f"SELECT COUNT(*) FROM {rel}")
        nulls = q(f"SELECT COUNT(*) FROM {rel} WHERE {measure_col} IS NULL") if measure_col else None
        hops.append({"relation": rel, "rows": rows, "measure": measure_col, "nulls": nulls})
    out["hops"] = hops
    con.close()
    return out


def fmt(v, width=18):
    return f"{v:>{width},.2f}" if isinstance(v, float) else f"{v:>{width},}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", choices=list(SCENARIOS), default=list(SCENARIOS))
    args = ap.parse_args()

    results = {}
    for name in args.only:
        print(f"building scenario: {name} {' '.join(SCENARIOS[name]) or '(no flags)'}",
              file=sys.stderr)
        build(SCENARIOS[name])
        results[name] = measure()

    names = list(results)

    print("\n" + "=" * 100)
    print("HEADLINE  rpt.desk_summary, firm-wide total_pnl_usd")
    print("=" * 100)
    print(f"{'scenario':10s} {'pnl @ ' + BUG_A_DATE:>20s} {'delta':>16s} "
          f"{'pnl @ ' + BUG_B_DATE:>20s} {'delta':>16s}")
    base = results.get("healthy")
    for n in names:
        r = results[n]
        da = (r["firm_pnl_bug_a_date"] - base["firm_pnl_bug_a_date"]) if base else 0.0
        db = (r["firm_pnl_bug_b_date"] - base["firm_pnl_bug_b_date"]) if base else 0.0
        print(f"{n:10s} {r['firm_pnl_bug_a_date']:>20,.2f} {da:>16,.2f} "
              f"{r['firm_pnl_bug_b_date']:>20,.2f} {db:>16,.2f}")

    print("\n" + "-" * 100)
    print("EVIDENCE")
    print("-" * 100)
    keys = ["fx_rate_rows", "null_base_amount", "residual_tier",
            "accrual_basis_rows", "null_accrual", "control_breaks",
            "firm_pnl_all_dates", "accrual_all_dates"]
    print(f"{'metric':22s}" + "".join(f"{n:>19s}" for n in names))
    for k in keys:
        print(f"{k:22s}" + "".join(fmt(results[n][k], 19) for n in names))

    print("\n" + "-" * 100)
    print("ROW COUNT AT EVERY HOP   (rows / rows with a NULL measure)")
    print("-" * 100)
    print(f"{'relation':26s}{'measure':22s}" + "".join(f"{n:>18s}" for n in names))
    for i, hop in enumerate(results[names[0]]["hops"]):
        cells = ""
        for n in names:
            h = results[n]["hops"][i]
            cells += f"{h['rows']:>11,} /{(h['nulls'] if h['nulls'] is not None else 0):>5,}"
        print(f"{hop['relation']:26s}{str(hop['measure'] or '-'):22s}{cells}")

    print("\n" + "-" * 100)
    print(f"WAREHOUSE STATE   left in the '{names[-1]}' state -- the last scenario built.")
    print("Re-seed before using it as a healthy baseline:")
    print("    python include/seed/generate.py && python include/runner.py")

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
