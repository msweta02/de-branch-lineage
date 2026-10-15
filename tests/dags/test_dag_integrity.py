"""Structural tests for the four module DAGs.

These run inside the Airflow image (`astro dev pytest`), where `airflow` and the
`include` package are importable. They assert the things that broke during
Phase 0 and would break silently again:

* every DAG file is discoverable by Airflow's safe-mode filter, which only opens
  files whose contents mention both "dag" and "airflow";
* every SQL task sits in the single-slot `duckdb_writer` pool, because DuckDB
  permits one writer and the asset chain can otherwise leave two module DAGs
  writing at once;
* the asset wiring actually forms the reference -> trades -> positions ->
  reporting chain.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from include.pipeline_dag import (
    MODULE_ASSETS,
    MODULE_SCHEDULES,
    POSITION_LAYER,
    REFERENCE_LAYER,
    REPORTING_LAYER,
    TRADE_LAYER,
    build_module_dag,
)
from include.runner import MODULES, module_files

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"

EXPECTED_TASK_COUNTS = {
    "01_reference": 5,
    "02_trades": 6,
    "03_positions": 5,
    "04_reporting": 4,
}


@pytest.mark.parametrize("module", MODULES)
def test_dag_builds_with_one_task_per_sql_file(module):
    dag = build_module_dag(module)
    assert len(dag.tasks) == len(module_files(module))
    assert len(dag.tasks) == EXPECTED_TASK_COUNTS[module]


@pytest.mark.parametrize("module", MODULES)
def test_tasks_are_a_single_chain_in_filename_order(module):
    dag = build_module_dag(module)
    expected = [p.stem.split("_", 1)[1] for p in module_files(module)]
    assert [t.task_id for t in dag.topological_sort()] == expected


@pytest.mark.parametrize("module", MODULES)
def test_every_sql_task_uses_the_duckdb_writer_pool(module):
    dag = build_module_dag(module)
    assert {t.pool for t in dag.tasks} == {"duckdb_writer"}


@pytest.mark.parametrize("module", MODULES)
def test_only_the_last_task_publishes_the_layer_asset(module):
    dag = build_module_dag(module)
    ordered = dag.topological_sort()
    for task in ordered[:-1]:
        assert not getattr(task, "outlets", None)
    assert [a.name for a in ordered[-1].outlets] == [MODULE_ASSETS[module].name]


def test_modules_form_the_expected_asset_chain():
    assert MODULE_SCHEDULES["02_trades"] == [REFERENCE_LAYER]
    assert MODULE_SCHEDULES["03_positions"] == [TRADE_LAYER]
    assert MODULE_SCHEDULES["04_reporting"] == [POSITION_LAYER]
    assert MODULE_ASSETS["04_reporting"] == REPORTING_LAYER
    # Reference is the only module on a clock rather than on an asset.
    assert isinstance(MODULE_SCHEDULES["01_reference"], str)


@pytest.mark.parametrize("path", sorted(DAGS_DIR.glob("*_pipeline.py")))
def test_dag_files_survive_airflow_safe_mode_discovery(path):
    """Airflow only opens a file if its contents mention both 'dag' and 'airflow'.

    A DAG file that imports its DAG from a helper module and never says
    "airflow" is skipped silently -- no error, no warning, no DAG. This cost
    real time in Phase 0; see docs/build-log.md, Surprise 7.
    """
    content = path.read_text().lower()
    assert "dag" in content
    assert "airflow" in content


@pytest.mark.parametrize("path", sorted(DAGS_DIR.glob("*_pipeline.py")))
def test_dag_files_are_valid_python(path):
    import ast

    ast.parse(path.read_text())
