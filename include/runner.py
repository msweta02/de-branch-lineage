#!/usr/bin/env python3
"""Executes the SQL modules under ``include/sql`` against DuckDB in DAG order.

Plumbing, not product. The Airflow tasks in ``dags/`` call :func:`run_file`,
and this module is also runnable standalone so the pipeline can be exercised
without bringing up Airflow::

    python include/runner.py                 # all four modules
    python include/runner.py 02_trades       # one module

Each ``.sql`` file holds exactly one statement and declares its output relation
and its inputs in a header comment::

    -- target: trd.trades_converted
    -- depends: trd.trades_enriched, ref.fx_rates

Those headers are the dependency declaration. ``branchlin.sources.rawfiles``
will read the same headers in Phase 1, which is why the runner parses them here
rather than inferring order from the filename prefixes alone.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import duckdb

SQL_ROOT = Path(__file__).resolve().parent / "sql"
DEFAULT_DB = Path(__file__).resolve().parent / "data" / "warehouse.duckdb"

MODULES = ["01_reference", "02_trades", "03_positions", "04_reporting"]

_TARGET_RE = re.compile(r"^--\s*target:\s*(\S+)", re.MULTILINE)
_DEPENDS_RE = re.compile(r"^--\s*depends:\s*(.+)$", re.MULTILINE)


def header(sql_text: str) -> tuple[str | None, list[str]]:
    """Pull the declared target relation and inputs out of the header comment."""
    m = _TARGET_RE.search(sql_text)
    target = m.group(1) if m else None
    d = _DEPENDS_RE.search(sql_text)
    depends = [x.strip() for x in d.group(1).split(",")] if d else []
    return target, depends


def module_files(module: str) -> list[Path]:
    return sorted((SQL_ROOT / module).glob("*.sql"))


def run_file(con: duckdb.DuckDBPyConnection, path: Path) -> dict:
    """Execute one .sql file and return a row-count record for it."""
    sql_text = path.read_text()
    target, depends = header(sql_text)
    t0 = time.perf_counter()
    con.execute(sql_text)
    elapsed = time.perf_counter() - t0
    rows = None
    if target:
        rows = con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0]
    return {
        "file": f"{path.parent.name}/{path.name}",
        "target": target,
        "depends": depends,
        "rows": rows,
        "seconds": round(elapsed, 3),
    }


def run_module(con: duckdb.DuckDBPyConnection, module: str, quiet: bool = False) -> list[dict]:
    results = []
    for path in module_files(module):
        r = run_file(con, path)
        results.append(r)
        if not quiet:
            print(f"  {r['file']:44s} -> {str(r['target']):26s} "
                  f"{r['rows']:>10,} rows  {r['seconds']:>6.2f}s")
    return results


def run_all(db: Path, modules: list[str] | None = None, quiet: bool = False) -> list[dict]:
    con = duckdb.connect(str(db))
    out = []
    try:
        for module in (modules or MODULES):
            if not quiet:
                print(f"\n[{module}]")
            out.extend(run_module(con, module, quiet=quiet))
    finally:
        con.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("modules", nargs="*", default=None,
                    help=f"modules to run, in order (default: {' '.join(MODULES)})")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    args = ap.parse_args()

    db = Path(args.db)
    if not db.exists():
        print(f"no warehouse at {db} -- run include/seed/generate.py first", file=sys.stderr)
        return 1

    t0 = time.perf_counter()
    run_all(db, args.modules or None)
    print(f"\ntotal {time.perf_counter() - t0:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
