"""Does each hop balance?

For every hop the tracer visits, the reconciler answers one question: does
the output of this hop account for its input? Concretely -- do the rows in
equal the rows out plus the rows each branch is *supposed* to drop, and does
the measure (a sum, a count, a net amount) survive the transformation.

A hop that balances is exonerated: whatever went wrong happened upstream.
The first hop that does *not* balance is the root cause, and everything
upstream of it is marked untrusted -- not because it is known to be wrong,
but because the trace reached it through logic that is itself broken, so its
evidence cannot be relied upon.

**The check that finds both planted bugs is null introduction, not arithmetic.**
Phase 0 established that row counts are identical at every hop under BUG A, and
that the recon control -- which reconciles counts and totals -- passes clean.
The signal that moves is a NULL appearing in a column whose inputs had none.
Arithmetic reconciliation is still done where it is meaningful (a hop that is a
pure ``SUM`` must equal the sum of its inputs, since summing sums is
associative) but it is deliberately not the only check, because on this
pipeline it would find nothing.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from branchlin import parser, pushdown
from branchlin.parser import BranchPoint, ParsedStatement
from branchlin.prober import Probe

# Verdicts.
BALANCED = "BALANCED"
BREAK = "BREAK"
UNTRUSTED = "UNTRUSTED"
APPROXIMATE = "APPROXIMATE"

#: Absolute tolerance when comparing two money figures, in currency units.
#: A cent, because the warehouse stores DOUBLE and the pipeline is all
#: multiplication -- exact equality would fail on representation alone.
TOLERANCE = 0.01


@dataclass(frozen=True)
class Check:
    """One question asked of one hop."""

    name: str
    passed: bool
    detail: str
    #: A failed fatal check makes the hop a BREAK. Non-fatal checks are
    #: reported as evidence but do not on their own condemn a hop.
    fatal: bool = False
    #: True when the check could not be evaluated -- reported, never fatal.
    skipped: bool = False

    def describe(self) -> str:
        if self.skipped:
            mark = "-"
        elif self.passed:
            mark = "ok"
        else:
            mark = "FAIL" if self.fatal else "warn"
        return f"[{mark:>4}] {self.name}: {self.detail}"


@dataclass(frozen=True)
class Verdict:
    """The reconciler's answer for one hop."""

    verdict: str
    checks: tuple[Check, ...] = ()
    reason: str = ""
    approximate: bool = False
    #: The branch points that plausibly caused a BREAK, most specific first.
    blame: tuple[BranchPoint, ...] = ()
    #: The step (CTE) the break localises to.
    step: str | None = None

    @property
    def is_break(self) -> bool:
        return self.verdict == BREAK

    @property
    def failures(self) -> tuple[Check, ...]:
        return tuple(c for c in self.checks if c.fatal and not c.passed)

    def describe(self) -> str:
        head = self.verdict
        if self.approximate and self.verdict != APPROXIMATE:
            head += " (approximate)"
        return f"{head}: {self.reason}" if self.reason else head


# ---------------------------------------------------------------------------
# blame attribution
# ---------------------------------------------------------------------------


def attribute(parsed: ParsedStatement, column: str) -> tuple[tuple[BranchPoint, ...], str]:
    """Which branch points could have broken ``column``, and in which step.

    Walks from the step that computes the column. An outer join in that step
    can null-extend it, so those come first. When such a join reads a CTE
    defined in the same statement, that CTE's filters are included too: a row
    missing from the joined set is *why* the join null-extended, and the filter
    is what removed it. That is how BUG B localises to the ``accrual_basis``
    CTE's WHERE rather than merely to the join that exposed it.
    """
    step_name = pushdown.computing_step(parsed, column)
    step = parsed.step(step_name)
    if step is None:
        return (), step_name

    step_names = {s.name for s in parsed.steps}
    blame: list[BranchPoint] = []

    outer_joins = [
        bp for bp in step.branch_points
        if bp.kind == parser.JOIN and bp.can_null_extend
    ]

    # Filters inside a CTE that an outer join in this step reads. These are the
    # most specific explanation available, so they lead.
    for join in outer_joins:
        joined = (join.joined_relation or "").split(" ")[0]
        if joined in step_names:
            upstream_step = parsed.step(joined)
            if upstream_step is not None:
                blame.extend(
                    bp for bp in upstream_step.branch_points
                    if bp.kind in (parser.WHERE, parser.HAVING, parser.QUALIFY)
                )

    blame.extend(outer_joins)

    # Filters in the computing step itself.
    blame.extend(
        bp for bp in step.branch_points
        if bp.kind in (parser.WHERE, parser.HAVING, parser.QUALIFY)
    )

    return tuple(blame), step_name


# ---------------------------------------------------------------------------
# the checks
# ---------------------------------------------------------------------------


def _check_selector_matched(output: Probe) -> Check:
    return Check(
        name="selector matched rows",
        passed=output.rows > 0,
        detail=(
            f"{output.rows:,} rows matched"
            if output.rows
            else "selector matched no rows at all -- nothing to reconcile"
        ),
        fatal=True,
    )


def _check_nulls_introduced(
    output: Probe,
    inputs: dict[tuple[str, str], Probe],
    null_safe: frozenset[tuple[str, str]] = frozenset(),
) -> Check:
    """A NULL in the output that its inputs cannot account for.

    The check that finds both planted bugs. Row counts do not move; this does.

    Compared as a *rate*, not a count. Two hops in this pipeline make counts
    useless on their own: netting expands 14,727 rows into 19,779, which
    multiplies existing NULLs without introducing any, and aggregation collapses
    410 NULL rows into 96 NULL groups. Both are legitimate. A null *fraction*
    that rises across a hop is not.

    Inputs consumed under COALESCE are excluded: they cannot transmit a NULL
    however many they hold, so letting them explain the output's NULLs
    exonerates the guilty hop.
    """
    if output.column is None or output.nulls is None:
        return Check("nulls introduced", True, "no column under trace", skipped=True)

    # Keyed by (relation, column): one relation can contribute several columns
    # to the same measure, and keying by relation alone silently drops all but
    # the last -- which produced a false BREAK. See BUILD_LOG "Surprise 18".
    measured = {
        key: p for key, p in inputs.items()
        if p.column is not None and p.nulls is not None and key not in null_safe
    }

    out_rate = (output.nulls / output.rows) if output.rows else 0.0

    if not measured:
        if not output.nulls:
            return Check("nulls introduced", True, "no nulls, no comparable input",
                         skipped=True)
        excluded = " (all null-safe under COALESCE)" if null_safe else ""
        return Check(
            "nulls introduced",
            False,
            f"{output.nulls:,} NULL in {output.column} and no input can account "
            f"for them{excluded}",
            fatal=True,
        )

    rates = {
        key: ((p.nulls / p.rows) if p.rows else 0.0) for key, p in measured.items()
    }
    best = max(rates.values())
    shape = ", ".join(
        f"{rel}.{col}={measured[(rel, col)].nulls:,}/{measured[(rel, col)].rows:,}"
        for rel, col in sorted(measured)
    )

    # Presence, not magnitude. Getting this right took three attempts and the
    # two failures are worth recording, because both were plausible:
    #
    #   * comparing counts blamed pos.positions_netted under BUG A -- netting
    #     expands 14,727 rows into 19,779 and multiplies existing NULLs with
    #     them, so the count rises without anything going wrong.
    #   * comparing *rates* blamed the same hop, because the expansion is not
    #     uniform: null-bearing rows there have more counterparties, so the
    #     null fraction legitimately climbs from 14.2% to 22.0%.
    #
    # Any row-multiplying hop breaks both comparisons in both directions. What
    # is actually diagnostic is whether *any* input could have supplied a NULL
    # at all -- which is why excluding COALESCE-wrapped inputs above matters so
    # much: with the calendar's single LAG-boundary NULL wrongly counted as an
    # explanation, BUG B's guilty hop was exonerated. See BUILD_LOG
    # "Surprise 19".
    if output.nulls and best == 0.0:
        return Check(
            "nulls introduced",
            False,
            f"{output.nulls:,} NULL in {output.column} "
            f"({out_rate:.1%} of {output.rows:,} rows) where inputs had none "
            f"({shape})",
            fatal=True,
        )

    if output.nulls and out_rate > best * 2 and out_rate - best > 0.05:
        # Reported, never fatal: a row-multiplying hop can do this legitimately.
        return Check(
            "nulls introduced",
            False,
            f"{output.nulls:,} NULL in {output.column} ({out_rate:.1%}) is well "
            f"above the {best:.1%} its inputs carry ({shape}) -- inherited, but "
            f"amplified here",
            fatal=False,
        )

    if output.nulls:
        return Check(
            "nulls introduced",
            True,
            f"{output.nulls:,} nulls in {output.column} ({out_rate:.1%}), and "
            f"inputs already carried {best:.1%} -- inherited, not introduced here",
        )
    return Check("nulls introduced", True, f"no nulls in {output.column}")


def _check_measure_sum(
    output: Probe,
    inputs: dict[tuple[str, str], Probe],
    origins: tuple[pushdown.MeasureOrigin, ...],
    tolerance: float,
) -> Check:
    """A pure-SUM hop must equal the sum of its inputs.

    Only meaningful when the output measure is a bare ``SUM`` of exactly one
    upstream column: summing sums is associative, so the two must agree. Any
    row-wise arithmetic (``qty * price * rate``) makes the comparison
    meaningless and the check is skipped rather than faked.
    """
    if output.value is None or output.column is None:
        return Check("measure reconciles", True, "no numeric measure", skipped=True)

    sums = [o for o in origins if (o.aggregate or "").upper() == "SUM"]
    if len(sums) != 1 or len(origins) != 1:
        return Check(
            "measure reconciles",
            True,
            "not a pure SUM of one column -- arithmetic is not invertible here",
            skipped=True,
        )

    origin = sums[0]
    probe = inputs.get((origin.relation, origin.column))
    if probe is None or probe.value is None:
        return Check(
            "measure reconciles", True,
            f"no probe of {origin.relation}.{origin.column} to compare", skipped=True,
        )

    delta = output.value - probe.value
    ok = abs(delta) <= tolerance
    return Check(
        name="measure reconciles",
        passed=ok,
        detail=(
            f"output {output.value:,.2f} vs SUM({origin.relation}.{origin.column}) "
            f"{probe.value:,.2f}  delta {delta:,.2f}"
        ),
        fatal=True,
    )


def _check_row_counts(
    output: Probe, inputs: dict[tuple[str, str], Probe], aggregates: bool
) -> Check:
    """Reported for evidence, never fatal.

    Phase 0 proved row counts find neither bug: under BUG A every relation holds
    exactly the same number of rows as a healthy run. A row-count difference is
    also entirely legitimate here -- the netting hop expands 14,727 rows to
    19,779. So this is context for a human, not a verdict.
    """
    if not inputs:
        return Check("row counts", True, "no inputs probed", skipped=True)
    shape = ", ".join(
        f"{rel}.{col}={p.rows:,}" for (rel, col), p in sorted(inputs.items())
    )
    return Check(
        name="row counts",
        passed=True,
        detail=(
            f"out={output.rows:,} in: {shape}"
            + ("  (hop aggregates, so a drop is expected)" if aggregates else "")
        ),
    )


def _check_type_stable(
    output: Probe,
    inputs: dict[tuple[str, str], Probe],
    origins: tuple[pushdown.MeasureOrigin, ...],
) -> Check:
    """Type before and after. Informational: a widening cast is not a fault."""
    if output.column_type is None:
        return Check("type stable", True, "no column type", skipped=True)
    upstream = {
        p.column_type for p in inputs.values() if p.column_type is not None
    }
    if not upstream:
        return Check("type stable", True, f"{output.column_type} (no input type)",
                     skipped=True)
    if output.column_type in upstream:
        return Check("type stable", True, f"{output.column_type} unchanged")
    return Check(
        name="type stable",
        passed=True,
        detail=f"{', '.join(sorted(upstream))} -> {output.column_type}",
    )


# ---------------------------------------------------------------------------
# the verdict
# ---------------------------------------------------------------------------


def reconcile(
    *,
    output: Probe,
    inputs: dict[tuple[str, str], Probe],
    parsed: ParsedStatement | None = None,
    column: str | None = None,
    approximate: bool = False,
    aggregates: bool = False,
    tolerance: float = TOLERANCE,
) -> Verdict:
    """Decide whether one hop accounts for its inputs."""
    origins: tuple[pushdown.MeasureOrigin, ...] = ()
    if parsed is not None and column is not None:
        origins = pushdown.measure_inputs(parsed, column)

    null_safe = frozenset(
        (o.relation, o.column) for o in origins if o.null_safe
    )
    checks = (
        _check_selector_matched(output),
        _check_nulls_introduced(output, inputs, null_safe),
        _check_measure_sum(output, inputs, origins, tolerance),
        _check_row_counts(output, inputs, aggregates),
        _check_type_stable(output, inputs, origins),
    )

    failures = [c for c in checks if c.fatal and not c.passed]
    if failures:
        blame: tuple[BranchPoint, ...] = ()
        step = None
        if parsed is not None and column is not None:
            blame, step = attribute(parsed, column)
        return Verdict(
            verdict=BREAK,
            checks=checks,
            reason="; ".join(c.detail for c in failures),
            approximate=approximate,
            blame=blame,
            step=step,
        )

    if approximate:
        return Verdict(
            verdict=APPROXIMATE,
            checks=checks,
            reason="hop balances, but the selector was widened to reach it",
            approximate=True,
        )

    return Verdict(verdict=BALANCED, checks=checks, reason="inputs account for the output")


def untrusted(reason: str = "upstream of a break") -> Verdict:
    """The verdict for every hop beyond a break.

    Not "wrong" -- unexaminable. The trace arrived here through logic that is
    itself broken, so this hop's evidence cannot be relied upon either way.
    """
    return Verdict(verdict=UNTRUSTED, reason=reason)
