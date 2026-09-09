"""Structural and orchestration tests for DIAGNOSE and REMEDIATE.

Two kinds of test live here, because Phase 2's claims are of two kinds.

**Structure** asserts the contract the DAG shape encodes, and runs anywhere
Airflow is importable (`astro dev pytest tests/dags`): that the embargo is a
setup and the lift is a teardown, that the approval gate is genuinely an
ApprovalOperator with `notify` downstream of it, that the gate does *not* hold
the single DuckDB writer slot while it waits, and that everything which touches
the warehouse does.

**Orchestration** asserts what actually happens when a run fails halfway
through -- that the teardown lifts the embargo anyway and the gate is never
reached. That needs a real DAG run against the metadata database, so it is
skipped unless one is reachable. It passes in the scheduler container:

    docker exec -e PYTHONPATH=/usr/local/airflow:/usr/local/airflow/include \\
      <scheduler> python -m pytest /usr/local/airflow/tests/dags -q

It is skipped under `astro dev pytest`, which runs a one-off container with no
metadata database and, separately, without the `.git` mount (see
docs/BUILD_LOG.md, Surprise 23).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

AIRFLOW_HOME = Path("/usr/local/airflow")
BREAK_RELATION = "trd.trades_converted"
GATE = "approve_external_notification"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(str(AIRFLOW_HOME / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag


@pytest.fixture(scope="module")
def remediate_dag(dagbag):
    dag = dagbag.dags.get("remediate")
    assert dag is not None, "remediate DAG did not load"
    return dag


@pytest.fixture(scope="module")
def diagnose_dag(dagbag):
    dag = dagbag.dags.get("diagnose")
    assert dag is not None, "diagnose DAG did not load"
    return dag


def task(dag, task_id):
    return dag.get_task(task_id)


# ---------------------------------------------------------------------------
# DIAGNOSE structure
# ---------------------------------------------------------------------------


class TestDiagnoseStructure:
    def test_it_takes_the_four_tracer_inputs_as_params(self, diagnose_dag):
        for name in ("relation", "selector", "column", "as_of"):
            assert name in diagnose_dag.params, f"missing param {name}"

    def test_the_trace_task_holds_the_writer_pool(self, diagnose_dag):
        """A read-only probe is not exempt from DuckDB's file lock.

        Measured in Phase 1: a read_only connection opened while a pipeline task
        holds the file is refused outright. See BUILD_LOG "Surprise 16".
        """
        assert task(diagnose_dag, "investigate").pool == "duckdb_writer"

    def test_remediation_is_opt_in(self, diagnose_dag):
        """A diagnosis is read far more often than it is acted on."""
        assert diagnose_dag.params["trigger_remediate"] is False

    def test_it_can_trigger_remediate_but_only_behind_a_gate(self, diagnose_dag):
        short_circuit = task(diagnose_dag, "break_found")
        trigger = task(diagnose_dag, "trigger_remediate")
        assert trigger.task_id in short_circuit.downstream_task_ids
        assert trigger.trigger_dag_id == "remediate"


# ---------------------------------------------------------------------------
# REMEDIATE structure -- the contract the shape encodes
# ---------------------------------------------------------------------------


class TestRemediateStructure:
    def test_the_embargo_is_a_setup(self, remediate_dag):
        assert task(remediate_dag, "embargo").is_setup

    def test_the_lift_is_a_teardown(self, remediate_dag):
        """The most important task in Phase 2.

        Being a teardown is what makes it run when something upstream failed.
        A plain task would be skipped, and the embargo would stay on.
        """
        assert task(remediate_dag, "lift_embargo").is_teardown

    def test_the_teardown_runs_regardless_of_upstream_outcome(self, remediate_dag):
        """ALL_DONE_SETUP_SUCCESS -- Airflow's teardown rule, and the right one.

        Stronger than a plain ALL_DONE: the lift runs once everything upstream
        has finished *however* it finished, but only if the setup that placed
        the embargo actually succeeded. If the embargo was never set there is
        nothing to lift, and running the teardown anyway would be a no-op at
        best and a release of some other run's hold at worst.
        """
        from airflow.task.trigger_rule import TriggerRule

        lift = task(remediate_dag, "lift_embargo")
        assert lift.trigger_rule == TriggerRule.ALL_DONE_SETUP_SUCCESS

    def test_the_teardown_is_downstream_of_the_setup_and_the_work(self, remediate_dag):
        lift = task(remediate_dag, "lift_embargo")
        assert "embargo" in lift.upstream_task_ids
        assert "notify" in lift.upstream_task_ids

    def test_the_gate_is_an_approval_operator(self, remediate_dag):
        from airflow.providers.standard.operators.hitl import ApprovalOperator

        gate = task(remediate_dag, GATE)
        assert isinstance(gate, ApprovalOperator)
        assert set(gate.options) == {"Approve", "Reject"}

    def test_the_gate_is_not_bypassable(self, remediate_dag):
        """Notification is downstream of approval, so rejection skips it.

        Telling an external consumer that a published number was wrong is the
        one genuinely irreversible action here. Nothing may reach `notify`
        without passing through the gate.
        """
        gate = task(remediate_dag, GATE)
        notify = task(remediate_dag, "notify")
        assert notify.task_id in gate.downstream_task_ids
        assert gate.task_id in notify.upstream_task_ids
        assert notify.upstream_task_ids == {gate.task_id}

    def test_the_gate_does_not_hold_the_writer_slot_while_it_waits(self, remediate_dag):
        """A parked task in a one-slot pool would block the whole warehouse.

        The gate sits in `awaiting_input` until a human answers -- possibly for
        hours. It touches no warehouse, so it must not take the slot.
        """
        assert task(remediate_dag, GATE).pool == "default_pool"

    def test_every_warehouse_task_holds_the_writer_slot(self, remediate_dag):
        for task_id in ("embargo", "correction", "recompute", "verify_report",
                        "measure", "notify", "lift_embargo"):
            assert task(remediate_dag, task_id).pool == "duckdb_writer", (
                f"{task_id} touches the warehouse without the writer pool"
            )

    def test_graph_only_tasks_do_not_hold_the_writer_slot(self, remediate_dag):
        """blast_radius reads .sql files off disk and no warehouse at all."""
        for task_id in ("blast_radius", "affected_report_list"):
            assert task(remediate_dag, task_id).pool == "default_pool"

    def test_the_recompute_is_not_dynamically_mapped(self, remediate_dag):
        """It is a fold, not a map, and deliberately so.

        Mapped instances are not scheduled in map-index order, so a mapped
        recompute rebuilt reports from stale inputs and "corrected" the figure
        to the original wrong number. See BUILD_LOG "Surprise 24".
        """
        assert not task(remediate_dag, "recompute").is_mapped

    def test_the_verification_is_dynamically_mapped(self, remediate_dag):
        """Mapping is used where the units genuinely are independent."""
        assert task(remediate_dag, "verify_report").is_mapped

    def test_the_order_of_the_workflow(self, remediate_dag):
        """embargo -> radius -> correction -> recompute -> verify -> measure -> gate -> notify."""
        expected = [
            ("embargo", "blast_radius"),
            ("blast_radius", "correction"),
            ("correction", "recompute"),
            ("recompute", "verify_report"),
            ("verify_report", "measure"),
            ("measure", GATE),
            (GATE, "notify"),
        ]
        for upstream, downstream in expected:
            assert downstream in task(remediate_dag, upstream).downstream_task_ids, (
                f"{upstream} -> {downstream} is not wired"
            )


# ---------------------------------------------------------------------------
# Orchestration: the failure path (Task 4)
# ---------------------------------------------------------------------------


def _metadata_db_reachable() -> bool:
    """Is there a *migrated* Airflow metadata database behind this process?

    Deliberately queries `dag_run` rather than `SELECT 1`. The
    `astro dev pytest` container has no Postgres configured and falls back to an
    empty SQLite file, against which `SELECT 1` happily succeeds -- so the
    cheap check reported a usable database and every test below then failed on
    "no such table: dag_run". Probing the table that is actually needed is the
    difference between skipping cleanly and erroring confusingly.
    """
    try:
        from airflow.utils.session import create_session
        from sqlalchemy import text

        with create_session() as session:
            session.execute(text("SELECT COUNT(*) FROM dag_run")).fetchone()
        return True
    except Exception:
        return False


requires_db = pytest.mark.skipif(
    not _metadata_db_reachable(),
    reason=(
        "no migrated Airflow metadata database reachable -- run these in the "
        "scheduler container (see this module's docstring)"
    ),
)


def _run_dag_test(conf: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["airflow", "dags", "test", "remediate", "--conf", json.dumps(conf)],
        capture_output=True, text=True, cwd=str(AIRFLOW_HOME),
    )


def _latest_run(dag_id: str = "remediate"):
    from airflow.utils.session import create_session
    from sqlalchemy import text

    with create_session() as session:
        run = session.execute(text(
            "SELECT run_id, state FROM dag_run WHERE dag_id = :d "
            "ORDER BY start_date DESC LIMIT 1"
        ), {"d": dag_id}).fetchone()
        if run is None:
            return None, {}
        states = {
            (r[0], r[1]): r[2] for r in session.execute(text(
                "SELECT task_id, map_index, state FROM task_instance "
                "WHERE dag_id = :d AND run_id = :r"
            ), {"d": dag_id, "r": run[0]}).fetchall()
        }
    flat: dict[str, set[str]] = {}
    for (task_id, _map_index), state in states.items():
        flat.setdefault(task_id, set()).add(state)
    return {"run_id": run[0], "state": run[1]}, flat


def _run_state(run_id: str) -> str | None:
    from airflow.utils.session import create_session
    from sqlalchemy import text

    with create_session() as session:
        row = session.execute(text(
            "SELECT state FROM dag_run WHERE dag_id = 'remediate' AND run_id = :r"
        ), {"r": run_id}).fetchone()
    return row[0] if row else None


def _warehouse():
    import duckdb
    from branchlin.prober import resolve_warehouse

    return duckdb.connect(str(resolve_warehouse().path), read_only=True)


def _build_warehouse(*flags: str) -> None:
    """Seed the warehouse with ``flags`` and run the whole pipeline over it."""
    subprocess.run(
        [sys.executable, str(AIRFLOW_HOME / "include" / "seed" / "generate.py"), *flags],
        check=True, capture_output=True,
    )
    subprocess.run(
        [sys.executable, str(AIRFLOW_HOME / "include" / "runner.py")],
        check=True, capture_output=True,
    )


@requires_db
class TestFailurePath:
    """A remediation that dies mid-recompute must still leave a clean state.

    This is the test that argues for an orchestrator rather than a script. A
    script would need its own `finally` and would still lose if the process were
    killed; Airflow's teardown semantics run the embargo lift either way, and
    the run is still reported as failed rather than being papered over by a
    successful cleanup.
    """

    @pytest.fixture(scope="class")
    @staticmethod
    def failed_run():
        """Run REMEDIATE with the recompute rigged to fail, once for the class.

        Seeds BUG A first and restores a healthy warehouse afterwards. The DAG
        resolves the real warehouse, so it cannot be handed a private copy --
        the cleanup is what keeps this test from leaving the project broken.
        """
        _build_warehouse("--bug-a")
        try:
            _run_dag_test({
                "break_relation": BREAK_RELATION,
                "original_value": 501622.18,
                "fail_recompute": True,
            })
            yield _latest_run()
        finally:
            _build_warehouse()

    def test_the_run_is_reported_as_failed(self, failed_run):
        """A successful cleanup must not make a failed remediation look fine."""
        run, _ = failed_run
        assert run["state"] == "failed"

    def test_the_recompute_failed(self, failed_run):
        _, states = failed_run
        assert "failed" in states["recompute"]

    def test_the_embargo_was_set(self, failed_run):
        _, states = failed_run
        assert states["embargo"] == {"success"}

    def test_the_teardown_ran_anyway(self, failed_run):
        """The whole point. The lift is a teardown, so failure does not skip it."""
        _, states = failed_run
        assert states["lift_embargo"] == {"success"}

    def test_the_embargo_is_actually_lifted_in_the_warehouse(self, failed_run):
        """Not just that the task ran -- that the control table says publishable."""
        from branchlin import remediate as R

        con = _warehouse()
        try:
            assert R.embargoed_relations(con) == ()
        finally:
            con.close()

    def test_the_approval_gate_was_never_reached(self, failed_run):
        """No human is asked to approve a remediation that did not complete."""
        _, states = failed_run
        assert "success" not in states[GATE]
        assert "awaiting_input" not in states[GATE]
        assert states[GATE] == {"upstream_failed"}

    def test_nothing_was_notified(self, failed_run):
        """The irreversible action did not happen."""
        from branchlin import remediate as R

        _, states = failed_run
        assert "success" not in states["notify"]

        run, _ = failed_run
        con = _warehouse()
        try:
            assert R.notifications_for(con, run["run_id"]) == ()
        finally:
            con.close()

    def test_the_state_left_behind_is_reasonable(self, failed_run):
        """A human must be able to reason about where this stopped.

        After a failed remediation: nothing is embargoed, so publication is not
        blocked forever; the reports still hold the *original* wrong figures
        rather than half-recomputed ones; and the remediation log records
        exactly which relations were rebuilt before the failure.
        """
        from branchlin import remediate as R

        run, _ = failed_run
        con = _warehouse()
        try:
            assert R.embargoed_relations(con) == ()
            figure = R.measure(
                con, "rpt.desk_summary", "total_pnl_usd",
                "business_date = DATE '2026-09-03'",
            )
            assert figure == pytest.approx(501622.18, abs=0.01), (
                "the report should still show the original wrong figure, not a "
                "partially recomputed one"
            )
            assert R.recomputed_in_run(con, run["run_id"]) == ()
        finally:
            con.close()


# ---------------------------------------------------------------------------
# Orchestration: the happy path (Task 5, steps 5-7)
# ---------------------------------------------------------------------------


def _api_base() -> str:
    """Where the API server is, from inside the containers.

    `AIRFLOW__API__BASE_URL` is `http://localhost:8080`, which is correct for a
    browser on the host and wrong from inside the scheduler -- the API server is
    a separate container. Use the compose service name.
    """
    import os

    return os.environ.get("BRANCHLIN_API_BASE", "http://api-server:8080")


def _api_token(username: str = "admin", password: str = "admin") -> str:
    import json as _json
    import urllib.request

    request = urllib.request.Request(
        f"{_api_base()}/auth/token",
        data=_json.dumps({"username": username, "password": password}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return _json.load(response)["access_token"]


def _approve(dag_run_id: str, task_id: str = GATE, option: str = "Approve") -> int:
    """Answer a parked approval gate through the same API the UI uses.

    Deliberately the real endpoint rather than a database write. Setting
    `hitl_detail.chosen_options` and flipping the task to `scheduled` by hand
    looks equivalent and is not: the resume path needs the `event` payload the
    endpoint attaches, and without it the gate fails with
    `execute_complete() missing 1 required positional argument: 'event'`.
    Tested through the API, the test exercises what a human clicking Approve
    actually does. See docs/BUILD_LOG.md, "Surprise 26".
    """
    import json as _json
    import urllib.parse
    import urllib.request

    encoded = urllib.parse.quote(dag_run_id, safe="")
    url = (
        f"{_api_base()}/api/v2/dags/remediate/dagRuns/{encoded}"
        f"/taskInstances/{task_id}/-1/hitlDetails"
    )
    request = urllib.request.Request(
        url,
        data=_json.dumps({"chosen_options": [option]}).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {_api_token()}",
        },
        method="PATCH",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status


def _wait_for(predicate, *, timeout: float = 180.0, interval: float = 3.0):
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    return None


def _gate_awaiting() -> str | None:
    from airflow.utils.session import create_session
    from sqlalchemy import text

    with create_session() as session:
        row = session.execute(text(
            "SELECT dr.run_id FROM dag_run dr JOIN task_instance ti "
            "ON ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id "
            "WHERE dr.dag_id = 'remediate' AND ti.task_id = :t "
            "AND ti.state = 'awaiting_input' "
            "ORDER BY dr.start_date DESC LIMIT 1"
        ), {"t": GATE}).fetchone()
    return row[0] if row else None


@requires_db
class TestHappyPath:
    """Correct the data, recompute, approve, and publish 607,617.49.

    The whole Phase 0 -> Phase 2 arc in one run: a report short by 105,995.31,
    traced to a LEFT JOIN, corrected, recomputed in dependency order, held back
    from publication throughout, released by a human decision, and restated to
    the figure the healthy pipeline produces.
    """

    @pytest.fixture(scope="class")
    @staticmethod
    def approved_run():
        import subprocess as sp

        _build_warehouse("--bug-a")
        try:
            sp.run(
                ["airflow", "dags", "trigger", "remediate", "--conf", json.dumps({
                    "break_relation": BREAK_RELATION,
                    "original_value": 501622.18,
                })],
                check=True, capture_output=True, text=True, cwd=str(AIRFLOW_HOME),
            )
            run_id = _wait_for(_gate_awaiting)
            assert run_id, "the approval gate never parked in awaiting_input"

            # Evidence captured while parked -- it is gone once the run finishes.
            from airflow.utils.session import create_session
            from sqlalchemy import text

            with create_session() as session:
                body = session.execute(text(
                    "SELECT h.body FROM hitl_detail h JOIN task_instance ti "
                    "ON ti.id = h.ti_id WHERE ti.run_id = :r AND ti.task_id = :t"
                ), {"r": run_id, "t": GATE}).fetchone()[0]

            assert _approve(run_id) == 200

            finished = _wait_for(lambda: _run_state(run_id) != "running")
            assert finished, "the run never finished after approval"
            yield {"run_id": run_id, "body": body, "state": _run_state(run_id)}
        finally:
            _build_warehouse()

    def test_the_gate_parked_before_notifying_anyone(self, approved_run):
        """The irreversible step waited for a human."""
        assert approved_run["body"]

    def test_the_evidence_showed_both_figures_and_the_delta(self, approved_run):
        body = approved_run["body"]
        assert "501,622.18" in body
        assert "607,617.49" in body
        assert "105,995.31" in body

    def test_the_evidence_named_the_break_and_the_code_version(self, approved_run):
        body = approved_run["body"]
        assert BREAK_RELATION in body
        assert "converted" in body
        # Either a real commit or an explicit UNPINNED -- never a silent gap.
        assert ("UNPINNED" in body) or ("via run-xcom" in body) or ("via conf" in body)

    def test_the_run_succeeded(self, approved_run):
        assert approved_run["state"] == "success"

    def test_the_report_now_reads_the_corrected_figure(self, approved_run):
        from branchlin import remediate as R

        con = _warehouse()
        try:
            figure = R.measure(
                con, "rpt.desk_summary", "total_pnl_usd",
                "business_date = DATE '2026-09-03'",
            )
        finally:
            con.close()
        assert figure == pytest.approx(607617.49, abs=0.01)

    def test_the_notification_fired_after_approval(self, approved_run):
        from branchlin import remediate as R

        con = _warehouse()
        try:
            sent = R.notifications_for(con, approved_run["run_id"])
        finally:
            con.close()
        assert len(sent) == 1
        assert sent[0][1] == "MOCK", "a demo must not send anything real"

    def test_the_embargo_was_lifted(self, approved_run):
        from branchlin import remediate as R

        con = _warehouse()
        try:
            assert R.embargoed_relations(con) == ()
        finally:
            con.close()

    def test_the_whole_blast_radius_was_recomputed_in_order(self, approved_run):
        """Starting from the corrected relation, not from the break."""
        from branchlin import remediate as R

        con = _warehouse()
        try:
            rebuilt = R.recomputed_in_run(con, approved_run["run_id"])
        finally:
            con.close()
        assert rebuilt[0] == "ref.fx_rates"
        assert BREAK_RELATION in rebuilt
        assert rebuilt.index(BREAK_RELATION) < rebuilt.index("rpt.desk_summary")
        assert rebuilt[-1].startswith("rpt.")
