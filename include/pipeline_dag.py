"""Builds one Airflow DAG per SQL module.

Each of the four modules under ``include/sql`` becomes a DAG whose tasks are
its ``.sql`` files, run in filename order, one task per file. The modules are
wired to each other with Assets rather than with cross-DAG sensors: reporting
waits on positions, which waits on trades, which waits on reference.

Assets are used on purpose. The eventual REMEDIATE flow walks the asset graph
to compute the blast radius of a bad figure, so the dependency structure has to
be something Airflow can traverse, not something implied by a schedule that
happens to run in the right order.
"""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, task

# Resolved at parse time. Inside the image this is /usr/local/airflow/include.
INCLUDE_DIR = Path(__file__).resolve().parent
WAREHOUSE = INCLUDE_DIR / "data" / "warehouse.duckdb"

# One asset per layer. The URI names the DuckDB schema the layer writes.
REFERENCE_LAYER = Asset(name="reference_layer", uri="duckdb://warehouse/ref")
TRADE_LAYER = Asset(name="trade_layer", uri="duckdb://warehouse/trd")
POSITION_LAYER = Asset(name="position_layer", uri="duckdb://warehouse/pos")
REPORTING_LAYER = Asset(name="reporting_layer", uri="duckdb://warehouse/rpt")

MODULE_ASSETS = {
    "01_reference": REFERENCE_LAYER,
    "02_trades": TRADE_LAYER,
    "03_positions": POSITION_LAYER,
    "04_reporting": REPORTING_LAYER,
}

MODULE_SCHEDULES = {
    "01_reference": "0 2 * * *",
    "02_trades": [REFERENCE_LAYER],
    "03_positions": [TRADE_LAYER],
    "04_reporting": [POSITION_LAYER],
}

MODULE_DOCS = {
    "01_reference": "Calendar, FX rates, book hierarchy, counterparty master.",
    "02_trades": "Ingest, dedupe, amend, enrich, convert to USD, classify.",
    "03_positions": "Aggregate to positions, corporate actions, MTM, accrual, netting.",
    "04_reporting": "Daily P&L, regulatory extract, desk summary, recon control.",
}


def _task_id(sql_path: Path) -> str:
    """`05_convert_currency.sql` -> `convert_currency`."""
    stem = sql_path.stem
    return stem.split("_", 1)[1] if "_" in stem else stem


def build_module_dag(module: str):
    """Create the DAG for one SQL module."""
    from include.runner import module_files, run_file

    sql_files = module_files(module)
    outlet = MODULE_ASSETS[module]

    @dag(
        dag_id=f"{module}_pipeline",
        schedule=MODULE_SCHEDULES[module],
        start_date=pendulum.datetime(2026, 8, 10, tz="UTC"),
        catchup=False,
        max_active_runs=1,          # see the duckdb_writer pool on each task
        tags=["de-branch-lineage", module],
        doc_md=MODULE_DOCS[module],
        default_args={"retries": 0},
    )
    def _module_dag():
        previous = None
        for i, sql_path in enumerate(sql_files):
            is_last = i == len(sql_files) - 1

            @task(
                task_id=_task_id(sql_path),
                outlets=[outlet] if is_last else None,
                # DuckDB permits a single writer per file. max_active_runs
                # serialises runs of one DAG, but the four module DAGs are
                # separate and the asset chain can leave two of them writing at
                # once -- which fails with "Conflicting lock is held". A global
                # one-slot pool serialises every SQL task across all four.
                pool="duckdb_writer",
            )
            def run_sql(path: str = str(sql_path)):
                """Execute one .sql file against the DuckDB warehouse."""
                import duckdb

                con = duckdb.connect(str(WAREHOUSE))
                try:
                    result = run_file(con, Path(path))
                finally:
                    con.close()
                print(f"{result['file']} -> {result['target']}: "
                      f"{result['rows']:,} rows in {result['seconds']}s")
                return result

            current = run_sql()
            if previous is not None:
                previous >> current
            previous = current

    return _module_dag()
