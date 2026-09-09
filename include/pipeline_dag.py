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



def _record_code_version() -> str | None:
    """Push this run's commit into XCom under ``code_version``.

    **Why XCom and not DagRun.conf.** ``branchlin.pinning`` prefers, in order,
    Airflow's own ``DagRun.bundle_version``, then a self-recorded
    ``conf['code_version']``, then the working tree. The middle step was
    specified as writing to the run's conf, and on Airflow 3.3 a task cannot do
    that: task execution goes through the API server, ``conf`` is set at trigger
    time, and the Task SDK exposes no endpoint to mutate it. Checked rather than
    assumed -- there is no such method on the SDK's DagRun, and nothing in
    ``airflow.sdk`` writes conf.

    XCom is the per-run store that does persist, is visible in the UI next to
    the task that wrote it, and is readable from the DIAGNOSE task. ``conf``
    remains supported for the case it genuinely works: a human triggering a run
    manually and supplying the version themselves.

    Never fatal. A pipeline that cannot read git still has to produce its data;
    an unrecorded version degrades a later trace to UNPINNED, which is labelled
    honestly, rather than failing the run that had nothing to do with it.
    """
    try:
        from branchlin import pinning

        version = pinning.head()
    except Exception as exc:  # no git binary, no .git mount, detached state
        print(f"code_version unrecorded: {type(exc).__name__}: {exc}")
        return None

    try:
        from airflow.sdk import get_current_context

        get_current_context()["ti"].xcom_push(key="code_version", value=version)
    except Exception as exc:
        print(f"code_version {version[:12]} not pushed to XCom: {exc}")
        return version

    print(f"code_version {version[:12]} recorded for this run")
    return version


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
                """Execute one .sql file against the DuckDB warehouse.

                Also records the commit this run executed against, so that a
                diagnosis of this date can parse the SQL that actually ran
                rather than whatever is in the working tree today. That is the
                whole "why an orchestrator" argument, and it costs one probe of
                git per task.
                """
                import duckdb

                con = duckdb.connect(str(WAREHOUSE))
                try:
                    result = run_file(con, Path(path))
                finally:
                    con.close()
                print(f"{result['file']} -> {result['target']}: "
                      f"{result['rows']:,} rows in {result['seconds']}s")

                result["code_version"] = _record_code_version()
                return result

            current = run_sql()
            if previous is not None:
                previous >> current
            previous = current

    return _module_dag()
