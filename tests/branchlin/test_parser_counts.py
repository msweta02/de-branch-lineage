"""The branch-point counter, pinned to the figures the project quotes.

Every number in this file appears in the README or in
docs/STATUS_TECHNICAL.md section 3.6. Until Phase 1 they were produced by a
throwaway sqlglot script, which meant they could drift out of date silently as
the pipeline changed. This test is how they stop drifting.

Two paths matter and they are not the same length:

* the **causal** path -- the one BUG A's NULL actually travels along, which is
  what the documented figures describe. 11 relations, 10 hops.
* the **structural** longest path -- every edge the ``-- depends:`` headers
  declare, which is longer because ``trd.trades_converted`` also reads
  ``ref.validity_window``. 12 relations, 11 hops.

``ref.validity_window`` is on the structural path but not the causal one. It is
joined ``ON c.currency = w.ccy`` and contributes ``coverage_status``, not the
amount; Phase 0 verified it holds 5 identical rows under BUG A (GBP still
reports ``FULL_SPAN``). A fault in the FX rate cannot reach the report through
it. Distinguishing the two is the whole reason branch lineage beats a
dependency graph: the graph offers five paths here, and only one carries the
fault.
"""

from __future__ import annotations

import pytest

from branchlin import parser
from branchlin.sources.rawfiles import RawFilesSource

# The causal path for BUG A, as documented in STATUS_TECHNICAL section 3.3.
# Written out rather than derived from the graph, because "which path carries
# the fault" is a question about a column, not about the dependency edges --
# test_tracer.py asserts the tracer rediscovers exactly this list.
CAUSAL_PATH = (
    "ref.fx_rates",
    "trd.trades_converted",
    "trd.trades_classified",
    "pos.book_positions",
    "pos.positions_adjusted",
    "pos.positions_marked",
    "pos.positions_accrued",
    "pos.positions_netted",
    "rpt.pnl_daily",
    "rpt.regulatory_extract",
    "rpt.desk_summary",
)

# Documented figures, STATUS_TECHNICAL section 3.6.
DOC_RELATIONS = 11
DOC_EDGES = 10
DOC_CTE_STEPS = 30
DOC_CASE_ARMS = 50
DOC_JOINS = 11
DOC_WHERE = 8
DOC_TOTAL = 69


@pytest.fixture(scope="module")
def source() -> RawFilesSource:
    return RawFilesSource()


def test_source_sees_every_sql_file(source):
    """20 SQL files, 20 relations, every one with a -- target: header."""
    assert len(source.files()) == 20
    assert len(source.by_target) == 20


def test_causal_path_shape(source):
    """The documented path is 11 relations and 10 hops."""
    assert len(CAUSAL_PATH) == DOC_RELATIONS
    assert len(CAUSAL_PATH) - 1 == DOC_EDGES


def test_causal_path_is_a_real_path(source):
    """Every consecutive pair is a declared -- depends: edge.

    Guards against the constant above rotting into fiction if the pipeline
    changes: the path must still exist in the headers.
    """
    for upstream, downstream in zip(CAUSAL_PATH, CAUSAL_PATH[1:]):
        assert upstream in source.upstream(downstream), (
            f"{downstream} no longer declares a dependency on {upstream}"
        )


def test_documented_branch_point_counts(source):
    """69 branch points: 50 CASE arms, 11 join conditions, 8 WHERE conjuncts.

    This is the figure the README quotes as the size of the search space.
    """
    counts = source.count_branch_points(CAUSAL_PATH)
    assert counts["case_arms"] == DOC_CASE_ARMS
    assert counts["join"] == DOC_JOINS
    assert counts["where"] == DOC_WHERE
    assert counts["having"] == 0, "no HAVING anywhere in the pipeline"
    assert counts["documented"] == DOC_TOTAL
    assert counts["cte_steps"] == DOC_CTE_STEPS


def test_case_arms_split_into_when_and_else(source):
    """The 50 arms are 33 WHEN plus 17 ELSE."""
    counts = source.count_branch_points(CAUSAL_PATH)
    assert counts["case_when"] == 33
    assert counts["case_else"] == 17
    assert counts["case_when"] + counts["case_else"] == DOC_CASE_ARMS


def test_qualify_is_counted_but_outside_the_documented_69(source):
    """One QUALIFY sits on the causal path, in 03_mark_to_market.

    The documented 69 excludes it -- the original throwaway script never looked
    for QUALIFY. The true total on the causal path is 70. Pinned here so the
    discrepancy is recorded rather than rediscovered.
    """
    counts = source.count_branch_points(CAUSAL_PATH)
    assert counts["qualify"] == 1
    assert counts["total"] == DOC_TOTAL + 1

    marked = source.parse("pos.positions_marked")
    qualifies = marked.of_kind(parser.QUALIFY)
    assert len(qualifies) == 1
    assert "ROW_NUMBER()" in qualifies[0].expression
    assert qualifies[0].step == "official_marks"


def test_structural_longest_path_is_longer_than_the_causal_one(source):
    """The graph offers a longer path that carries no fault.

    12 relations via ref.validity_window. Pinned so the difference between
    "reachable" and "causal" stays visible.
    """
    structural = source.longest_path("ref.fx_rates", "rpt.desk_summary")
    assert len(structural) == 12
    assert "ref.validity_window" in structural
    assert "ref.validity_window" not in CAUSAL_PATH
    assert len(source.paths("ref.fx_rates", "rpt.desk_summary")) == 5


def test_validity_window_join_cannot_carry_the_amount(source):
    """Why validity_window is off the causal path, asserted not asserted-by-comment.

    Its join into trd.trades_converted keys on currency alone -- no date -- so
    it cannot transmit a per-date FX fault.
    """
    converted = source.parse("trd.trades_converted")
    joins = {bp.joined_relation.split(" ")[0]: bp for bp in converted.of_kind(parser.JOIN)}
    vw = joins["ref.validity_window"]
    assert "rate_date" not in vw.expression
    assert "trade_date" not in vw.expression
    assert vw.expression == "c.currency = w.ccy"


def test_bug_a_join_is_an_outer_join_that_can_null_extend(source):
    """The BUG A mechanism, as a property of the parse.

    A LEFT JOIN on (currency, trade_date) that can null-extend is exactly what
    column-level lineage does not report.
    """
    converted = source.parse("trd.trades_converted")
    fx = [bp for bp in converted.of_kind(parser.JOIN)
          if bp.joined_relation.startswith("ref.fx_rates")]
    assert len(fx) == 1
    assert fx[0].join_side == "LEFT"
    assert fx[0].can_null_extend is True
    assert "f.ccy" in fx[0].expression and "f.rate_date" in fx[0].expression


def test_bug_b_filter_is_a_half_open_window_inside_a_cte(source):
    """The BUG B mechanism: two WHERE conjuncts, counted separately."""
    accrued = source.parse("pos.positions_accrued")
    basis = accrued.step("accrual_basis")
    assert basis is not None
    wheres = [bp for bp in basis.branch_points if bp.kind == parser.WHERE]
    assert len(wheres) == 2, "the half-open window is two independent conjuncts"
    exprs = " | ".join(sorted(bp.expression for bp in wheres))
    assert ">=" in exprs and "<" in exprs
    assert "accrual_period_start" in exprs
    assert "accrual_period_end" in exprs


def test_positions_are_left_joined_to_the_basis_not_filtered_by_it(source):
    """Why BUG B does not delete the day -- the reason it went unnoticed."""
    accrued = source.parse("pos.positions_accrued")
    step = accrued.step("accrued")
    joins = [bp for bp in step.branch_points if bp.kind == parser.JOIN]
    basis_join = [bp for bp in joins if "accrual_basis" in (bp.joined_relation or "")]
    assert len(basis_join) == 1
    assert basis_join[0].join_side == "LEFT"
    assert basis_join[0].can_null_extend is True


def test_group_by_keys_are_available_for_pushdown(source):
    """Aggregation is the easy direction for pushdown -- the keys must be visible."""
    summary = source.parse("rpt.desk_summary")
    by_desk = summary.step("by_desk")
    assert by_desk.aggregates
    assert by_desk.group_by == (
        "business_date", "desk_code", "desk_name", "division",
    )


def test_every_statement_parses_and_declares_its_output(source):
    """No file in the pipeline is unparseable or headerless."""
    for st in source.ordered():
        parsed = source.parse(st.target)
        assert parsed.target == st.target
        assert parsed.steps, f"{st.origin} produced no steps"


def test_ordered_is_a_valid_topological_order(source):
    """Every statement appears after the statements it depends on."""
    seen: set[str] = set()
    for st in source.ordered():
        for dep in st.depends:
            if dep in source.by_target:
                assert dep in seen, f"{st.target} came before its input {dep}"
        seen.add(st.target)
    assert len(seen) == 20
