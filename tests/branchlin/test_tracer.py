"""The DIAGNOSE engine, proved against the Phase 0 rig.

Phase 0 planted two faults and proved they are measurable, attributable and
invisible to row counts and to the pipeline's own reconciliation control. These
tests are the other half of that argument: that the engine finds them, names
the branch responsible, and stays quiet when nothing is wrong.

Each scenario gets its own warehouse, built once per session (see conftest).
The traces run against the same 20 SQL files in every scenario -- the SQL is
byte-identical between healthy and broken, which is the property that makes
"the code didn't change, the reference data did" demonstrable.
"""

from __future__ import annotations

import pytest

from branchlin import parser, reconciler
from branchlin.tracer import EXPLAIN, INVESTIGATE

BUG_A_DATE = "2026-09-03"
BUG_B_DATE = "2026-08-31"

REPORT = "rpt.desk_summary"


def selector(date: str) -> str:
    return f"business_date = DATE '{date}'"


# The path documented in STATUS_TECHNICAL section 3.6, in the order the tracer
# walks it (report first). ref.fx_rates is the eleventh relation: the trace
# names it as the fault's origin rather than walking into it, because it is
# upstream of the break.
EXPECTED_WALK = (
    "rpt.desk_summary",
    "rpt.regulatory_extract",
    "rpt.pnl_daily",
    "pos.positions_netted",
    "pos.positions_accrued",
    "pos.positions_marked",
    "pos.positions_adjusted",
    "pos.book_positions",
    "trd.trades_classified",
    "trd.trades_converted",
)


# ---------------------------------------------------------------------------
# BUG A -- a JOIN failure
# ---------------------------------------------------------------------------


class TestBugA:
    @pytest.fixture(scope="class")
    @staticmethod
    def trace(warehouses, source):
        from branchlin.prober import Prober
        from branchlin.tracer import Tracer

        with Prober(warehouses["bug_a"]) as prober:
            yield Tracer(source, prober).investigate(
                REPORT, selector(BUG_A_DATE), "total_pnl_usd", as_of=BUG_A_DATE
            )

    def test_it_breaks(self, trace):
        assert trace.broke, "BUG A produced no break"

    def test_the_break_is_the_currency_conversion(self, trace):
        hop = trace.break_hop
        assert hop.relation == "trd.trades_converted"
        assert hop.origin == "02_trades/05_convert_currency.sql"

    def test_the_break_localises_to_the_converted_cte(self, trace):
        """Reported at relation granularity, localised to a CTE."""
        assert trace.break_hop.step == "converted"

    def test_the_blamed_branch_is_the_left_join_on_fx_rates(self, trace):
        """The specific finding: an outer join that null-extended."""
        blame = trace.break_hop.verdict.blame
        assert blame, "break named no responsible branch point"
        top = blame[0]
        assert top.kind == parser.JOIN
        assert top.join_side == "LEFT"
        assert top.can_null_extend is True
        assert top.joined_relation.startswith("ref.fx_rates")
        assert "rate_date" in top.expression

    def test_everything_upstream_is_untrusted(self, trace):
        """Not wrong -- unexaminable, because the trace would pass through the break."""
        assert "ref.fx_rates" in trace.untrusted
        assert "trd.trades_enriched" in trace.untrusted
        assert "raw.fx_rates" in trace.untrusted
        for relation in trace.untrusted:
            assert relation not in trace.relations

    def test_the_walk_is_the_documented_causal_path(self, trace):
        """Ten relations walked, plus ref.fx_rates named as the origin.

        The tracer rediscovers the path that STATUS_TECHNICAL section 3.6
        documents, without being told it: it follows the measure, not the
        dependency edges.
        """
        assert trace.relations == EXPECTED_WALK
        assert len(trace.relations) == 10
        assert trace.hop_count == 9
        assert len({*trace.relations, "ref.fx_rates"}) == 11

    def test_it_never_walks_into_validity_window(self, trace):
        """The structural path that carries no fault is not reported.

        ref.validity_window is a declared input of trd.trades_converted, so a
        dependency-graph walk would visit it. It joins on currency alone and
        contributes coverage_status, so the amount never passes through it.
        """
        assert "ref.validity_window" not in trace.relations

    def test_hops_before_the_break_all_balance(self, trace):
        """A hop that balances is exonerated -- the fault is upstream of it."""
        for hop in trace.hops[:-1]:
            assert hop.verdict.verdict in (
                reconciler.BALANCED, reconciler.APPROXIMATE
            ), f"{hop.relation} unexpectedly {hop.verdict.verdict}"

    def test_the_report_row_itself_is_clean(self, trace):
        """Nothing on the report shows a null, a warning or a missing row."""
        report = trace.hops[0]
        assert report.probe.nulls == 0
        assert report.probe.rows == 5
        assert report.probe.value == pytest.approx(501622.18, abs=0.01)

    def test_row_counts_do_not_reveal_it(self, trace, warehouses):
        """The Phase 0 property, restated as a property of the trace.

        Every relation on the path holds the same number of rows as a healthy
        run. Only the null count moves.
        """
        from branchlin.prober import Prober

        with Prober(warehouses["healthy"]) as healthy:
            for hop in trace.hops:
                clean = healthy.probe(hop.relation, hop.column, hop.selector or None)
                assert clean.rows == hop.probe.rows, (
                    f"{hop.relation} changed row count -- BUG A is supposed to "
                    f"be invisible to row counts"
                )

    def test_the_broken_hop_is_where_nulls_appear_from_nowhere(self, trace):
        failures = trace.break_hop.verdict.failures
        assert len(failures) == 1
        assert failures[0].name == "nulls introduced"
        assert "where inputs had none" in failures[0].detail

    def test_it_does_not_fire_on_the_other_date(self, warehouses, source):
        """BUG A is date-specific: 2026-08-31 is clean under it.

        Matches the Phase 0 delta table, where bug_a moves 2026-09-03 by
        -105,995.31 and 2026-08-31 by exactly zero.
        """
        from branchlin.prober import Prober
        from branchlin.tracer import Tracer

        with Prober(warehouses["bug_a"]) as prober:
            other = Tracer(source, prober).investigate(
                REPORT, selector(BUG_B_DATE), "accrued_interest_usd"
            )
        assert not other.broke


# ---------------------------------------------------------------------------
# BUG B -- a FILTER failure
# ---------------------------------------------------------------------------


class TestBugB:
    @pytest.fixture(scope="class")
    @staticmethod
    def trace(warehouses, source):
        from branchlin.prober import Prober
        from branchlin.tracer import Tracer

        with Prober(warehouses["bug_b"]) as prober:
            yield Tracer(source, prober).investigate(
                REPORT, selector(BUG_B_DATE), "accrued_interest_usd",
                as_of=BUG_B_DATE,
            )

    def test_it_breaks(self, trace):
        assert trace.broke, "BUG B produced no break"

    def test_the_break_is_the_accrual(self, trace):
        hop = trace.break_hop
        assert hop.relation == "pos.positions_accrued"
        assert hop.origin == "03_positions/04_accrue_interest.sql"

    def test_the_blamed_branch_is_the_accrual_basis_where(self, trace):
        """The specific finding required of BUG B.

        The WHERE lives in the accrual_basis CTE, one level below the LEFT JOIN
        that exposed it. Blaming the join alone would point at correct SQL; the
        filter inside the CTE it reads is what actually dropped the rows.
        """
        blame = trace.break_hop.verdict.blame
        wheres = [bp for bp in blame if bp.kind == parser.WHERE]
        assert wheres, "no WHERE conjunct blamed"
        assert all(bp.step == "accrual_basis" for bp in wheres)
        exprs = " | ".join(bp.expression for bp in wheres)
        assert "accrual_period_end" in exprs
        assert "accrual_period_start" in exprs

    def test_the_half_open_window_is_two_separate_conjuncts(self, trace):
        """Each conjunct can exclude a row on its own, so each is its own branch."""
        wheres = [bp for bp in trace.break_hop.verdict.blame if bp.kind == parser.WHERE]
        assert len(wheres) == 2

    def test_the_exposing_join_is_also_reported(self, trace):
        """The join is not the cause, but it is how the fault became visible."""
        joins = [bp for bp in trace.break_hop.verdict.blame if bp.kind == parser.JOIN]
        assert any("accrual_basis" in (bp.joined_relation or "") for bp in joins)
        assert all(bp.can_null_extend for bp in joins)

    def test_the_day_does_not_vanish_from_the_report(self, trace):
        """Why nobody noticed: the row survives, only the accrual is lost.

        All five desks still report on 2026-08-31 -- the day is present and the
        report looks complete. What is missing is the accrual, and because every
        contributing row was excluded, ``SUM`` over the group returns NULL
        rather than a wrong number.
        """
        report = trace.hops[0]
        assert report.probe.rows == 5, "the day vanished from the report"
        assert report.probe.nulls == 5
        assert report.probe.value is None

    def test_the_lost_accrual_is_the_documented_delta(self, warehouses):
        """The healthy figure for that date is exactly the Phase 0 delta.

        22,017.08 is what BUG B removes from 2026-08-31, and it is the entire
        accrual for the day -- so the delta is not a partial shortfall, it is
        the whole column going missing on one day a month.
        """
        from branchlin.prober import Prober

        with Prober(warehouses["healthy"]) as healthy:
            clean = healthy.probe(
                REPORT, "accrued_interest_usd", selector(BUG_B_DATE)
            )
        assert clean.rows == 5
        assert clean.nulls == 0
        assert clean.value == pytest.approx(22017.08, abs=0.01)

    def test_the_headline_total_still_looks_clean(self, warehouses):
        """total_pnl_usd carries no NULL -- which is why this went unnoticed."""
        from branchlin.prober import Prober

        with Prober(warehouses["bug_b"]) as prober:
            total = prober.probe(REPORT, "total_pnl_usd", selector(BUG_B_DATE))
        assert total.nulls == 0
        assert total.value == pytest.approx(663923.90, abs=0.01)

    def test_it_does_not_fire_on_the_other_date(self, warehouses, source):
        """BUG B moves 2026-08-31 only -- 2026-09-03 is clean under it."""
        from branchlin.prober import Prober
        from branchlin.tracer import Tracer

        with Prober(warehouses["bug_b"]) as prober:
            other = Tracer(source, prober).investigate(
                REPORT, selector(BUG_A_DATE), "total_pnl_usd"
            )
        assert not other.broke


# ---------------------------------------------------------------------------
# HEALTHY -- the engine must stay quiet
# ---------------------------------------------------------------------------


class TestHealthy:
    @pytest.mark.parametrize(
        "date,column",
        [
            (BUG_A_DATE, "total_pnl_usd"),
            (BUG_B_DATE, "accrued_interest_usd"),
            (BUG_A_DATE, "mtm_pnl_usd"),
        ],
    )
    def test_no_break_on_a_clean_warehouse(self, tracer_for, date, column):
        """A false positive would make the tool unusable."""
        trace = tracer_for("healthy").investigate(REPORT, selector(date), column)
        assert not trace.broke, (
            f"false break at {trace.break_hop.relation}: "
            f"{trace.break_hop.verdict.reason}"
        )

    def test_the_path_reaches_a_source_relation(self, tracer_for):
        """A complete trace ends at a raw landing table this pipeline reads."""
        trace = tracer_for("healthy").investigate(
            REPORT, selector(BUG_A_DATE), "total_pnl_usd"
        )
        last = trace.hops[-1]
        assert last.relation.startswith("raw."), (
            f"trace stopped at {last.relation}, short of a source relation"
        )
        assert last.primary is None
        assert "source relation" in last.verdict.reason

    def test_the_healthy_figure_is_the_documented_one(self, tracer_for):
        trace = tracer_for("healthy").investigate(
            REPORT, selector(BUG_A_DATE), "total_pnl_usd"
        )
        assert trace.hops[0].probe.value == pytest.approx(607617.49, abs=0.01)

    def test_untrusted_is_empty_when_nothing_broke(self, tracer_for):
        trace = tracer_for("healthy").investigate(
            REPORT, selector(BUG_A_DATE), "total_pnl_usd"
        )
        assert trace.untrusted == ()


# ---------------------------------------------------------------------------
# EXPLAIN -- walk to source, annotate the branches
# ---------------------------------------------------------------------------


class TestExplain:
    @pytest.fixture(scope="class")
    @staticmethod
    def trace(warehouses, source):
        from branchlin.prober import Prober
        from branchlin.tracer import Tracer

        with Prober(warehouses["healthy"]) as prober:
            yield Tracer(source, prober).explain(
                REPORT, selector(BUG_A_DATE), "total_pnl_usd"
            )

    def test_it_walks_to_source(self, trace):
        assert trace.mode == EXPLAIN
        assert not trace.broke
        assert len(trace.hops) > len(EXPECTED_WALK)
        assert trace.hops[-1].primary is None

    def test_it_reaches_the_raw_landing_tables(self, trace):
        """The point of EXPLAIN: the whole path, not just as far as a fault."""
        assert any(r.startswith("raw.") for r in trace.relations)

    def test_every_hop_carries_a_verdict_and_a_value(self, trace):
        for hop in trace.hops:
            assert hop.verdict.verdict in (
                reconciler.BALANCED, reconciler.APPROXIMATE
            )
            assert hop.probe is not None

    def test_it_records_which_file_and_step_produced_each_hop(self, trace):
        """What makes the output readable as an explanation rather than a dump."""
        produced = [h for h in trace.hops if h.origin is not None]
        assert len(produced) >= len(EXPECTED_WALK)
        for hop in produced:
            assert hop.origin.endswith(".sql")
            assert hop.step

    def test_non_obvious_branches_are_singled_out(self, trace):
        """Default view is the branches worth looking at, not all 69."""
        annotated = [h for h in trace.hops if h.branches]
        assert annotated, "no branches observed anywhere"
        total = sum(len(h.branches) for h in trace.hops)
        shown = sum(len(h.non_obvious_branches) for h in trace.hops)
        assert shown < total, "every branch was marked non-obvious -- no filtering"

    def test_an_else_arm_is_reported_as_non_obvious(self, trace):
        """Falling through to ELSE is exactly what a human wants told."""
        elses = [
            b for h in trace.hops for b in h.non_obvious_branches
            if b.branch.kind == parser.CASE_ELSE
        ]
        assert elses, "no ELSE arm reported"
        assert all(b.taken for b in elses)
        assert all(b.rows for b in elses)

    def test_the_rendering_is_readable(self, trace):
        out = trace.render()
        assert "EXPLAIN" in out
        assert "NO BREAK" in out
        assert "rpt.desk_summary" in out
        # one line per hop, plus provenance lines -- not a table dump
        assert len(out.splitlines()) < 120

    def test_explain_does_not_halt_on_a_broken_warehouse(self, warehouses, source):
        """Same machine, different halt condition -- that is the only difference."""
        from branchlin.prober import Prober
        from branchlin.tracer import Tracer

        with Prober(warehouses["bug_a"]) as prober:
            tracer = Tracer(source, prober)
            investigated = tracer.investigate(
                REPORT, selector(BUG_A_DATE), "total_pnl_usd"
            )
            explained = tracer.explain(
                REPORT, selector(BUG_A_DATE), "total_pnl_usd"
            )
        assert investigated.broke and explained.broke
        assert len(explained.hops) > len(investigated.hops), (
            "EXPLAIN stopped at the break like INVESTIGATE did"
        )
        assert explained.untrusted == ()


# ---------------------------------------------------------------------------
# the input contract
# ---------------------------------------------------------------------------


class TestInputContract:
    def test_a_bad_column_is_reported_with_the_available_ones(self, tracer_for):
        """The four inputs are a contract; a typo should say so clearly."""
        trace = tracer_for("healthy").investigate(
            REPORT, selector(BUG_A_DATE), "base_amount_usd"
        )
        assert trace.broke
        reason = trace.break_hop.verdict.reason
        assert "no column" in reason
        assert "base_amount_usd" in reason
        assert "business_date" in reason, "the error should list what does exist"

    def test_a_selector_matching_nothing_is_a_break_not_a_crash(self, tracer_for):
        trace = tracer_for("healthy").investigate(
            REPORT, "business_date = DATE '1999-01-01'", "total_pnl_usd"
        )
        assert trace.broke
        assert "no rows" in trace.break_hop.verdict.reason

    def test_the_selector_is_a_predicate_not_an_id(self, tracer_for):
        """Multi-column selectors are pushed as a conjunction."""
        trace = tracer_for("healthy").investigate(
            REPORT,
            f"business_date = DATE '{BUG_A_DATE}' AND desk_code = 'DSK-FX'",
            "total_pnl_usd",
        )
        assert not trace.broke
        assert trace.hops[0].probe.rows == 1
