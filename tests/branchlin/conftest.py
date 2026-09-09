"""Make branchlin importable, and build one warehouse per bug scenario.

The library lives under ``include/`` so that Airflow's DAG bundle carries it,
which means it is not on ``sys.path`` by default when pytest runs from the repo
root. The Airflow image already has ``/usr/local/airflow/include`` on the path,
so the path insertion only matters for host runs.

The scenario warehouses are built into a session-scoped temporary directory,
never over ``include/data/warehouse.duckdb``. Phase 0's harness re-seeds that
file in place, so a test suite that did the same would destroy whatever state a
developer was looking at -- and would make test results depend on the order
they ran in.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
INCLUDE = ROOT / "include"
if str(INCLUDE) not in sys.path:
    sys.path.insert(0, str(INCLUDE))

PYTHON = str(ROOT / ".venv" / "bin" / "python")
if not Path(PYTHON).exists():  # in-container runs use the image interpreter
    PYTHON = sys.executable

GENERATOR = INCLUDE / "seed" / "generate.py"
RUNNER = INCLUDE / "runner.py"

SCENARIOS = {
    "healthy": [],
    "bug_a": ["--bug-a"],
    "bug_b": ["--bug-b"],
}


def _build(db: Path, flags: list[str]) -> Path:
    """Seed a warehouse and run the whole pipeline against it."""
    subprocess.run(
        [PYTHON, str(GENERATOR), "--out", str(db), *flags],
        cwd=str(ROOT), check=True, capture_output=True,
    )
    subprocess.run(
        [PYTHON, str(RUNNER), "--db", str(db)],
        cwd=str(ROOT), check=True, capture_output=True,
    )
    return db


@pytest.fixture(scope="session")
def warehouses(tmp_path_factory) -> dict[str, Path]:
    """One built warehouse per scenario, shared across the session."""
    base = tmp_path_factory.mktemp("warehouses")
    return {
        name: _build(base / f"{name}.duckdb", flags)
        for name, flags in SCENARIOS.items()
    }


@pytest.fixture(scope="session")
def source():
    """The pipeline SQL from the working tree."""
    from branchlin.sources.rawfiles import RawFilesSource

    return RawFilesSource()


@pytest.fixture
def tracer_for(warehouses, source):
    """A tracer bound to one scenario's warehouse."""
    from branchlin.prober import Prober
    from branchlin.tracer import Tracer

    made = []

    def _make(scenario: str):
        prober = Prober(warehouses[scenario])
        made.append(prober)
        return Tracer(source, prober)

    yield _make
    for p in made:
        p.close()
