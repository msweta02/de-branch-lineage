"""The remediation mechanics, and the full correct-and-recompute sequence.

These tests cover everything about REMEDIATE that does not need a scheduler:
the embargo, the blast radius, the correction, the ordered recompute and the
mocked notification. The orchestration semantics -- that the teardown lifts the
embargo even when the recompute fails, and that the approval gate is never
reached -- need a real DAG run and live in tests/dags/test_remediate_dag.py.

Task 5's data assertions are proved here end to end: seed BUG A, apply the
correction, recompute, and read 607,617.49 back off the report.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from branchlin import remediate as R
from branchlin.sources.rawfiles import RawFilesSource

BUG_A_DATE = "2026-09-03"
BREAK_RELATION = "trd.trades_converted"
REPORT = "rpt.desk_summary"
COLUMN = "total_pnl_usd"
SELECTOR = f"business_date = DATE '{BUG_A_DATE}'"

WRONG = 501_622.18
CORRECT = 607_617.49
DELTA = 105_995.31

#: The correction the REMEDIATE DAG offers by default: the late redelivery of
#: the FX rate BUG A withheld. Kept in sync with dags/remediate.py by
#: test_the_dag_default_correction_matches.
CORRECTION = (
    "INSERT INTO raw.fx_rates "
    "(rate_date, ccy, quote_ccy, rate, rate_source, ingested_at) "
    "SELECT DATE '2026-09-03', 'GBP', 'USD', 1.270854, 'LATE_REDELIVERY', now() "
    "WHERE NOT EXISTS ("
    "  SELECT 1 FROM raw.fx_rates "
    "  WHERE rate_date = DATE '2026-09-03' AND ccy = 'GBP'"
    ")"
)


@pytest.fixture
def con(warehouses, tmp_path):
    """A writable copy of the BUG A warehouse, per test."""
    db = tmp_path / "remediate.duckdb"
    db.write_bytes(Path(warehouses["bug_a"]).read_bytes())
    connection = duckdb.connect(str(db))
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def source() -> RawFilesSource:
    return RawFilesSource()


def figure(connection) -> float | None:
    return R.measure(connection, REPORT, COLUMN, SELECTOR)


# ---------------------------------------------------------------------------
# embargo -- the control that has to be right
# ---------------------------------------------------------------------------


class TestEmbargo:
    def test_it_is_observable_with_a_select(self, con, source):
        """Not a lock. A human must be able to ask "is this safe to publish"."""
        assert R.embargoed_relations(con) == ()
        assert R.is_publishable(con, REPORT)

        R.set_embargo(con, [REPORT], run_id="r1", reason="test")

        assert REPORT in R.embargoed_relations(con)
        assert not R.is_publishable(con, REPORT)

    def test_lifting_releases_exactly_what_was_held(self, con, source):
        reports = R.affected_reports(source, BREAK_RELATION)
        R.set_embargo(con, reports, run_id="r1", reason="test")
        assert set(R.embargoed_relations(con)) == set(reports)

        released = R.lift_embargo(con, run_id="r1")

        assert set(released) == set(reports)
        assert R.embargoed_relations(con) == ()

    def test_lifting_is_scoped_to_one_run(self, con):
        """Two remediations must not release each other's holds."""
        R.set_embargo(con, ["rpt.desk_summary"], run_id="run-a", reason="a")
        R.set_embargo(con, ["rpt.pnl_daily"], run_id="run-b", reason="b")

        R.lift_embargo(con, run_id="run-a")

        assert R.embargoed_relations(con) == ("rpt.pnl_daily",)

    def test_lifting_is_idempotent(self, con):
        """A teardown may run more than once; it must not fail the second time."""
        R.set_embargo(con, [REPORT], run_id="r1", reason="test")
        assert R.lift_embargo(con, run_id="r1") == (REPORT,)
        assert R.lift_embargo(con, run_id="r1") == ()
        assert R.embargoed_relations(con) == ()

    def test_lifting_works_before_anything_was_ever_embargoed(self, con):
        """A teardown that throws is a teardown that leaves an embargo in place.

        The setup may have failed before it created the control table, and the
        teardown still has to run and still has to succeed.
        """
        con.execute(f"DROP TABLE IF EXISTS {R.EMBARGO_TABLE}")
        assert R.lift_embargo(con, run_id="never-set") == ()

    def test_the_lift_is_recorded_not_just_undone(self, con):
        """The audit trail: when it was held, when it was released."""
        R.set_embargo(con, [REPORT], run_id="r1", reason="because")
        R.lift_embargo(con, run_id="r1")
        row = con.execute(
            f"SELECT reason, set_at IS NOT NULL, lifted_at IS NOT NULL, embargoed "
            f"FROM {R.EMBARGO_TABLE} WHERE relation = ?", [REPORT],
        ).fetchone()
        assert row == ("because", True, True, False)


# ---------------------------------------------------------------------------
# blast radius
# ---------------------------------------------------------------------------


class TestBlastRadius:
    def test_it_is_derived_from_the_graph_not_hardcoded(self, source):
        affected = R.blast_radius(source, BREAK_RELATION)
        assert "trd.trades_classified" in affected
        assert REPORT in affected
        # nothing upstream of the break is in the downstream radius
        assert "trd.trades_enriched" not in affected
        assert "ref.fx_rates" not in affected

    def test_it_is_ordered_for_re_execution(self, source):
        """An ordering, not a set: inputs before the things that read them."""
        affected = R.blast_radius(source, BREAK_RELATION)
        assert affected.index("rpt.regulatory_extract") < affected.index(REPORT)
        assert affected.index("pos.book_positions") < affected.index("pos.positions_netted")

    def test_the_embargo_set_is_the_reports_only(self, source):
        reports = R.affected_reports(source, BREAK_RELATION)
        assert reports == (
            "rpt.pnl_daily", "rpt.regulatory_extract",
            "rpt.desk_summary", "rpt.recon_control",
        )

    def test_the_recompute_set_includes_the_break_relation(self, source):
        """The embargo and the recompute want different answers.

        The break relation consumed the bad input, so it is the one that will
        consume the corrected input. Leaving it out means the fix never enters
        the pipeline -- which happened, and produced a "corrected" figure
        identical to the wrong one.
        """
        assert BREAK_RELATION not in R.blast_radius(source, BREAK_RELATION)
        order = R.recompute_order(source, BREAK_RELATION)
        assert order[0] == BREAK_RELATION

    def test_a_source_relation_is_not_itself_recomputable(self, source):
        """raw.fx_rates has no statement, so it cannot be re-executed."""
        order = R.recompute_order(source, "raw.fx_rates")
        assert "raw.fx_rates" not in order
        assert order[0] == "ref.fx_rates"


# ---------------------------------------------------------------------------
# where the recompute must start
# ---------------------------------------------------------------------------


class TestCorrectionTarget:
    def test_it_parses_the_insert_target(self):
        assert R.correction_target(CORRECTION) == "raw.fx_rates"

    def test_it_parses_updates_and_deletes(self):
        assert R.correction_target(
            "UPDATE raw.calendar SET accrual_period_end = DATE '2026-09-01' "
            "WHERE calendar_date = DATE '2026-08-31'"
        ) == "raw.calendar"
        assert R.correction_target(
            "DELETE FROM raw.trades WHERE trade_id = 'TRD0000001'"
        ) == "raw.trades"

    def test_no_correction_means_no_target(self):
        assert R.correction_target(None) is None
        assert R.correction_target("") is None
        assert R.correction_target("   ") is None

    def test_unparseable_sql_does_not_raise(self):
        """A correction written by hand should not crash the remediation."""
        assert R.correction_target("this is not sql at all !!") is None

    def test_rebuild_starts_from_the_correction_not_the_break(self, source):
        """The bug this exists to prevent, stated as a test.

        The fix lands three relations upstream of the break. A recompute that
        starts at the break rebuilds ten relations and misses the two that
        mattered, and the figure does not move.
        """
        start = R.rebuild_from(source, CORRECTION, BREAK_RELATION)
        assert start == "raw.fx_rates"

        order = R.recompute_order(source, start)
        assert "ref.fx_rates" in order
        assert order.index("ref.fx_rates") < order.index(BREAK_RELATION)

    def test_it_falls_back_to_the_break_when_there_is_no_correction(self, source):
        """Data already fixed upstream: rebuild from the break."""
        assert R.rebuild_from(source, "", BREAK_RELATION) == BREAK_RELATION
        assert R.rebuild_from(source, None, BREAK_RELATION) == BREAK_RELATION


# ---------------------------------------------------------------------------
# the whole sequence -- Task 5's data assertions
# ---------------------------------------------------------------------------


class TestCorrectAndRecompute:
    def test_the_report_starts_wrong(self, con):
        assert figure(con) == pytest.approx(WRONG, abs=0.01)

    def test_correcting_the_data_alone_does_not_fix_the_report(self, con):
        """The reason a recompute is needed at all."""
        assert R.apply_correction(con, CORRECTION) == 1
        assert figure(con) == pytest.approx(WRONG, abs=0.01)

    def test_the_correction_is_idempotent(self, con):
        """A remediation may be retried; the rate must not land twice."""
        assert R.apply_correction(con, CORRECTION) == 1
        assert R.apply_correction(con, CORRECTION) == 0

    def test_correct_then_recompute_produces_the_documented_figure(self, con, source):
        """Seed BUG A, correct, recompute in order, read 607,617.49.

        The end of the Phase 0 story: the same figure the healthy pipeline
        produces, reached by remediating the broken one rather than by
        re-seeding it.
        """
        R.apply_correction(con, CORRECTION)
        start = R.rebuild_from(source, CORRECTION, BREAK_RELATION)
        for relation in R.recompute_order(source, start):
            R.recompute_relation(con, source, relation, run_id="test")

        assert figure(con) == pytest.approx(CORRECT, abs=0.01)

    def test_the_delta_is_the_documented_shortfall(self, con, source):
        before = figure(con)
        R.apply_correction(con, CORRECTION)
        for relation in R.recompute_order(
            source, R.rebuild_from(source, CORRECTION, BREAK_RELATION)
        ):
            R.recompute_relation(con, source, relation, run_id="test")
        after = figure(con)
        assert after - before == pytest.approx(DELTA, abs=0.01)

    def test_recomputing_out_of_order_does_not_fix_the_figure(self, con, source):
        """Why the recompute is a fold and not a map.

        Rebuilding the reports before their inputs rebuilds them from stale
        data. This is not hypothetical: the first implementation mapped over the
        affected set, Airflow ran the mapped instances out of index order, and
        the "corrected" figure came back identical to the wrong one.
        """
        R.apply_correction(con, CORRECTION)
        order = R.recompute_order(
            source, R.rebuild_from(source, CORRECTION, BREAK_RELATION)
        )
        for relation in reversed(order):
            R.recompute_relation(con, source, relation, run_id="backwards")

        assert figure(con) != pytest.approx(CORRECT, abs=0.01)

    def test_every_recompute_is_logged(self, con, source):
        """The audit trail of what was rebuilt, in the order it happened."""
        order = R.recompute_order(source, BREAK_RELATION)
        for relation in order:
            R.recompute_relation(con, source, relation, run_id="run-x")
        assert R.recomputed_in_run(con, "run-x") == order
        assert R.recomputed_in_run(con, "other-run") == ()

    def test_recomputing_something_unproducible_is_an_error(self, con, source):
        with pytest.raises(R.RemediationError, match="cannot be recomputed"):
            R.recompute_relation(con, source, "raw.trades", run_id="x")


# ---------------------------------------------------------------------------
# notification -- mocked, and provably so
# ---------------------------------------------------------------------------


class TestNotification:
    def test_it_records_rather_than_sends(self, con):
        """A demo must not be one misconfiguration from telling a regulator."""
        R.notify_external(
            con, run_id="r1", subject="Restatement", body="body",
            payload={"delta": DELTA},
        )
        sent = R.notifications_for(con, "r1")
        assert len(sent) == 1
        assert sent[0][1] == "MOCK"
        assert sent[0][2].endswith(".invalid")

    def test_nothing_is_recorded_for_a_run_that_did_not_notify(self, con):
        assert R.notifications_for(con, "never-happened") == ()


# ---------------------------------------------------------------------------
# the evidence a human is shown
# ---------------------------------------------------------------------------


class TestApprovalEvidence:
    def _evidence(self, **kw) -> R.ApprovalEvidence:
        base = dict(
            relation=REPORT, column=COLUMN, selector=SELECTOR,
            break_relation=BREAK_RELATION, break_step="converted",
            break_file="02_trades/05_convert_currency.sql",
            blamed_branch="LEFT JOIN ref.fx_rates ON p.currency = f.ccy",
            original_value=WRONG, corrected_value=CORRECT,
            code_version="b5e8748df0c1a323b6bcc594d8f41515f91ed035",
            code_provenance="run-xcom",
            recomputed=("ref.fx_rates", BREAK_RELATION, REPORT),
            embargoed=(REPORT,),
        )
        base.update(kw)
        return R.ApprovalEvidence(**base)

    def test_it_states_the_delta(self):
        assert self._evidence().delta == pytest.approx(DELTA, abs=0.01)

    def test_it_presents_everything_the_brief_requires(self):
        """The break, both figures, the delta, and the code version."""
        body = self._evidence().render()
        assert "501,622.18" in body            # published, wrong
        assert "607,617.49" in body            # corrected
        assert "105,995.31" in body            # delta
        assert BREAK_RELATION in body          # the break
        assert "converted" in body             # localised to the CTE
        assert "b5e8748df0c1" in body          # code version the original ran under
        assert "run-xcom" in body              # and how that was established

    def test_it_says_what_approving_and_rejecting_do(self):
        """An irreversible action needs its consequences spelled out."""
        body = self._evidence().render()
        assert "Approving records an external notification" in body
        assert "notifies no one" in body
        assert "embargo lifts" in body

    def test_an_unpinned_code_version_is_labelled_not_hidden(self):
        body = self._evidence(code_version=None, code_provenance=None).render()
        assert "UNPINNED" in body

    def test_an_unknown_figure_does_not_render_as_a_number(self):
        body = self._evidence(corrected_value=None).render()
        assert "unknown" in body


# ---------------------------------------------------------------------------
# the DAG and the library must not drift apart
# ---------------------------------------------------------------------------


def test_the_dag_default_correction_matches_this_test():
    """The correction in dags/remediate.py is the one proved above.

    Compared as normalised text rather than imported, because importing the DAG
    module needs Airflow, which the host venv does not have.
    """
    dag_src = (Path(__file__).resolve().parents[2] / "dags" / "remediate.py").read_text()
    start = dag_src.index("DEFAULT_CORRECTION = (")
    end = dag_src.index(")", dag_src.index('")', start)) + 1
    literal = dag_src[start:end]
    joined = "".join(
        line.strip().strip('"') for line in literal.splitlines()[1:]
    ).replace('" "', "")
    normalise = lambda s: " ".join(s.replace('"', "").split())
    assert normalise(CORRECTION) == normalise(joined)
