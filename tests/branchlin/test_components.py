"""Prober, pushdown and pinning, tested apart from the walk.

The tracer tests prove the engine finds the planted bugs. These prove the
individual mechanisms behave as specified even where the walk would not
exercise them -- in particular the widening rules, which matter precisely in
the cases a demo does not hit.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from branchlin import pinning, pushdown
from branchlin.parser import FINAL_STEP
from branchlin.prober import (
    REQUIRES_WRITER_POOL,
    WRITER_POOL,
    ProbeError,
    Prober,
    resolve_warehouse,
)

BUG_A_DATE = "2026-09-03"


# ---------------------------------------------------------------------------
# prober
# ---------------------------------------------------------------------------


class TestProber:
    def test_it_never_selects_star(self, warehouses):
        """A trace's cost must scale with hops, not with data volume."""
        with Prober(warehouses["healthy"]) as p:
            p.probe("rpt.desk_summary", "total_pnl_usd",
                    f"business_date = DATE '{BUG_A_DATE}'")
            p.distinct_values("rpt.desk_summary", "desk_code")
            assert p.issued
            for sql in p.issued:
                assert "SELECT *" not in sql.upper()

    def test_one_query_answers_value_rows_and_nulls(self, warehouses):
        """Three facts, one pass over the data."""
        with Prober(warehouses["healthy"]) as p:
            probe = p.probe("rpt.desk_summary", "total_pnl_usd",
                            f"business_date = DATE '{BUG_A_DATE}'")
            assert probe.rows == 5
            assert probe.nulls == 0
            assert probe.value == pytest.approx(607617.49, abs=0.01)
            assert probe.column_type == "DOUBLE"
        assert len([q for q in p.issued if q.startswith("SELECT COUNT(*)")]) == 1

    def test_it_counts_nulls_where_they_exist(self, warehouses):
        """The signal the whole engine turns on."""
        with Prober(warehouses["bug_a"]) as p:
            probe = p.probe("trd.trades_converted", "base_amount")
            assert probe.nulls == 410
            assert probe.rows == 49_508

    def test_a_non_numeric_column_is_counted_but_not_summed(self, warehouses):
        with Prober(warehouses["healthy"]) as p:
            probe = p.probe("trd.trades_converted", "conversion_status")
            assert probe.rows == 49_508
            assert probe.value is None
            assert probe.nulls == 0

    def test_unknown_relation_and_column_are_clear_errors(self, warehouses):
        with Prober(warehouses["healthy"]) as p:
            with pytest.raises(ProbeError, match="does not exist"):
                p.probe("rpt.nope", "x")
            with pytest.raises(ProbeError, match="no column"):
                p.probe("rpt.desk_summary", "nope")

    def test_identifiers_are_validated(self, warehouses):
        """Relation and column names shape the query, so they are checked."""
        with Prober(warehouses["healthy"]) as p:
            with pytest.raises(ProbeError, match="not a valid"):
                p.probe("rpt.desk_summary; DROP TABLE x", "total_pnl_usd")

    def test_distinct_values_is_bounded(self, warehouses):
        """A high-cardinality column must not turn a trace into a table dump."""
        with Prober(warehouses["healthy"]) as p:
            rows = p.distinct_values("trd.trades_converted", "trade_id", limit=5)
            assert len(rows) == 5

    def test_a_reader_still_needs_the_writer_pool(self, warehouses):
        """Verified, not assumed -- and the opposite of the natural guess.

        DuckDB locks the *file*, so a read-only connection opened while a writer
        holds it is refused. A DIAGNOSE task must therefore take the same
        one-slot pool as a pipeline task.
        """
        import duckdb

        assert REQUIRES_WRITER_POOL is True
        assert WRITER_POOL == "duckdb_writer"

        db = warehouses["healthy"]
        writer = duckdb.connect(str(db))
        try:
            probe = subprocess.run(
                [sys.executable, "-c",
                 "import duckdb,sys\n"
                 "try:\n"
                 "    duckdb.connect(sys.argv[1], read_only=True)\n"
                 "    print('OPENED')\n"
                 "except Exception as e:\n"
                 "    print('REFUSED', 'lock' in str(e).lower())\n",
                 str(db)],
                capture_output=True, text=True,
            )
            assert "REFUSED True" in probe.stdout, (
                f"expected the reader to be locked out, got: {probe.stdout!r}"
            )
        finally:
            writer.close()

    def test_warehouse_resolution_order(self, monkeypatch, warehouses):
        """Explicit argument, then environment, then Airflow, then repo default."""
        explicit = resolve_warehouse(warehouses["healthy"])
        assert explicit.provenance == "argument"

        monkeypatch.setenv("BRANCHLIN_WAREHOUSE", str(warehouses["bug_a"]))
        assert resolve_warehouse().provenance == "environment"

        monkeypatch.delenv("BRANCHLIN_WAREHOUSE")
        # No Airflow on the host path, so this falls through to the repo default
        # rather than failing -- a trace must be runnable from a plain shell.
        assert resolve_warehouse().provenance in ("repo-default", "airflow:duckdb_warehouse")


# ---------------------------------------------------------------------------
# pushdown
# ---------------------------------------------------------------------------


class TestPushdown:
    def test_aggregation_pushes_exactly(self, source):
        """A GROUP BY key means the upstream selector is the same predicate."""
        parsed = source.parse("rpt.desk_summary")
        out = pushdown.push(parsed, f"business_date = DATE '{BUG_A_DATE}'")
        result = out["rpt.regulatory_extract"]
        assert result.exact
        assert not result.widened
        assert "business_date" in result.selector
        assert result.mapped == (("business_date", "business_date"),)

    def test_a_rename_inside_a_cte_is_followed(self, source):
        """trade_date AS business_date, one level below the GROUP BY.

        The rename that matters most in this pipeline: a selector written
        against the report's column names has to arrive upstream as
        ``trade_date``.
        """
        parsed = source.parse("pos.book_positions")
        resolved = pushdown.resolve_column(parsed, "business_date")
        assert resolved.relation == "trd.trades_classified"
        assert resolved.column == "trade_date"
        assert resolved.exact
        assert "trade_level" in resolved.via

        out = pushdown.push(parsed, f"business_date = DATE '{BUG_A_DATE}'")
        pushed = out["trd.trades_classified"]
        assert pushed.exact
        assert "trade_date" in pushed.selector
        assert "business_date" not in pushed.selector

    def test_a_selector_on_an_aggregate_widens_rather_than_fails(self, source):
        """SUM(x) = 5 cannot be pushed to the rows that were summed."""
        parsed = source.parse("rpt.desk_summary")
        out = pushdown.push(parsed, "trade_count = 42")
        result = out["rpt.regulatory_extract"]
        assert not result.exact
        assert result.unrestricted
        assert any(reason == pushdown.AGGREGATE for _, reason in result.dropped)

    def test_a_selector_on_a_window_result_widens(self, source):
        """pnl_rank_in_day comes from RANK() OVER -- widened to the partition."""
        parsed = source.parse("rpt.desk_summary")
        out = pushdown.push(parsed, "pnl_rank_in_day = 1")
        result = out["rpt.regulatory_extract"]
        assert not result.exact
        assert any(reason == pushdown.WINDOW for _, reason in result.dropped)

    def test_a_selector_on_a_case_result_widens(self, source):
        """size_tier is a CASE result: the arms are known, the value is not invertible."""
        parsed = source.parse("trd.trades_classified")
        out = pushdown.push(parsed, "size_tier = 'TIER_5_RESIDUAL'")
        result = out["trd.trades_converted"]
        assert not result.exact
        assert any(reason == pushdown.CASE for _, reason in result.dropped)

    def test_widening_keeps_the_pushable_part(self, source):
        """A mixed selector pushes what it can and drops only the rest.

        This is what makes widening useful rather than merely safe: the date
        still narrows the search even though the aggregate cannot.
        """
        parsed = source.parse("rpt.desk_summary")
        out = pushdown.push(
            parsed, f"business_date = DATE '{BUG_A_DATE}' AND trade_count = 42"
        )
        result = out["rpt.regulatory_extract"]
        assert not result.exact
        assert "business_date" in result.selector
        assert "trade_count" not in result.selector

    def test_measure_lineage_follows_through_aggregates(self, source):
        """What column-level lineage gives, used as a substrate."""
        parsed = source.parse("pos.book_positions")
        origins = pushdown.measure_inputs(parsed, "gross_amount_usd")
        assert len(origins) == 1
        assert origins[0].relation == "trd.trades_classified"
        assert origins[0].column == "base_amount"
        assert origins[0].aggregate == "SUM"

    def test_computing_step_finds_where_a_value_comes_into_being(self, source):
        """Not where it is projected -- where it is calculated."""
        converted = source.parse("trd.trades_converted")
        assert pushdown.computing_step(converted, "base_amount") == "converted"

        accrued = source.parse("pos.positions_accrued")
        assert pushdown.computing_step(accrued, "accrued_interest_usd") == "accrued"

        summary = source.parse("rpt.desk_summary")
        assert pushdown.computing_step(summary, "total_pnl_usd") == "by_desk"

    def test_coalesce_wrapped_inputs_are_marked_null_safe(self, source):
        """A column consumed under COALESCE cannot transmit a NULL.

        ref.calendar.day_count_from_prev is NULL at the window boundary and is
        wrapped in COALESCE. Counting it as an explanation for downstream NULLs
        exonerated BUG B's guilty hop.
        """
        parsed = source.parse("pos.positions_accrued")
        origins = pushdown.measure_inputs(parsed, "accrued_interest_usd")
        calendar = [o for o in origins if o.relation == "ref.calendar"]
        assert calendar, "expected the calendar to feed the accrual"
        assert all(o.null_safe for o in calendar)
        others = [o for o in origins if o.relation == "pos.positions_marked"]
        assert others and not any(o.null_safe for o in others)

    def test_no_selector_means_no_restriction(self, source):
        parsed = source.parse("rpt.desk_summary")
        out = pushdown.push(parsed, None)
        assert all(r.unrestricted for r in out.values())
        assert all(r.exact for r in out.values())


# ---------------------------------------------------------------------------
# pinning -- which code ran
# ---------------------------------------------------------------------------


SQL_LANDED = "b293ac6"      # the commit that added include/sql
BEFORE_SQL = "48f7df8"      # the scaffold commit, before it existed


@pytest.mark.skipif(not pinning.git_available(), reason="no git history available")
class TestPinning:
    def test_it_materialises_sql_at_a_historical_commit(self):
        pinned = pinning.source_at(SQL_LANDED)
        assert pinned.is_pinned
        assert pinned.version == SQL_LANDED
        assert len(pinned.source.by_target) == 20
        assert pinned.source.parse("trd.trades_converted").target == "trd.trades_converted"

    def test_the_checkout_is_genuinely_version_pinned(self):
        """The proof: a commit before the SQL existed must not yield SQL.

        Without this, a "pinned" read could quietly be the working tree.
        """
        with pytest.raises(pinning.PinningError, match="include/sql"):
            pinning.source_at(BEFORE_SQL)

    def test_reading_history_does_not_mutate_the_repository(self):
        """git archive, not git worktree: a diagnosis leaves no trace behind.

        Goes through ``pinning._git`` rather than calling git directly. Inside
        the containers the repository is owned by another uid, and a bare
        ``git status`` is refused with "detected dubious ownership" -- the same
        trap the library had to handle, which this test originally walked
        straight into.
        """
        before = pinning._git("status", "--porcelain")
        pinning.source_at(SQL_LANDED)
        after = pinning._git("status", "--porcelain")
        assert before == after

    def test_an_unpinned_trace_says_so(self):
        """A trace must never claim a provenance it does not have."""
        wt = pinning.working_tree_source()
        assert not wt.is_pinned
        assert wt.provenance == pinning.WORKING_TREE
        assert "UNPINNED" in wt.describe()

    def test_it_falls_back_rather_than_failing(self):
        """Whatever the environment offers, the result is usable and labelled.

        Off Airflow this resolves to the working tree; inside the containers,
        where the pipeline records its commit, it resolves to a real sha. Both
        are correct -- what matters is that a version is never invented and the
        provenance always says which happened.
        """
        resolved = pinning.source_for("2026-09-03")
        assert resolved.source.by_target
        assert resolved.provenance in (
            pinning.WORKING_TREE,
            pinning.BUNDLE,
            pinning.RUN_CONF,
            pinning.RUN_XCOM,
            pinning.RUN_XCOM_LATEST,
        )
        assert resolved.is_pinned == (resolved.provenance != pinning.WORKING_TREE)

    def test_a_pinned_source_traces_like_any_other(self, warehouses):
        """The whole point: the engine runs against historical SQL unchanged."""
        from branchlin.prober import Prober
        from branchlin.tracer import Tracer

        pinned = pinning.source_at(SQL_LANDED)
        with Prober(warehouses["bug_a"]) as prober:
            trace = Tracer(
                pinned.source, prober, source_version=pinned.version
            ).investigate(
                "rpt.desk_summary", f"business_date = DATE '{BUG_A_DATE}'",
                "total_pnl_usd", as_of=BUG_A_DATE,
            )
        assert trace.broke
        assert trace.break_hop.relation == "trd.trades_converted"
        assert trace.source_version == SQL_LANDED
        assert SQL_LANDED in trace.render()
