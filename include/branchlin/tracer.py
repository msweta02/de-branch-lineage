"""Orchestrates the backward walk.

Entry point for DIAGNOSE. Given a wrong value in a report -- a table, a
measure, and a selector identifying the row -- the tracer walks upstream in
reverse dependency order, asking :mod:`branchlin.prober` for evidence and
:mod:`branchlin.reconciler` for a verdict at each hop, and halts at the first
imbalance.

Two modes, one machine:

* **INVESTIGATE** -- halt at the first BREAK, mark everything upstream
  UNTRUSTED. Used when a number is known to be wrong.
* **EXPLAIN** -- never halt. Walk to source, annotating every hop with the
  branch the row took and the value it carried. Used to understand a pipeline
  that is not broken: onboarding, audit, "why is this trade classified as
  large".

Only the halt condition differs. Same parser, same prober, same reconciler.

**The path is discovered, not declared.** The walk follows the *measure*
backwards through :func:`branchlin.pushdown.measure_inputs`, not the
``-- depends:`` graph. That matters: the dependency graph offers five paths
from ``ref.fx_rates`` to ``rpt.desk_summary`` and only one of them carries the
amount. ``ref.validity_window`` is a declared input of
``trd.trades_converted`` but contributes ``coverage_status``, so a trace of
``base_amount`` never visits it. Following edges would report a path that
cannot transmit the fault; following the measure does not.

**What the prober can and cannot see.** Evidence is only available at relation
boundaries, because a CTE is not materialised -- there is no table to probe.
So the walk reasons about CTE steps *statically* (which step computes the
column, which branch points sit in it) and gathers *evidence* at relation
granularity. A break is therefore localised to a CTE by analysis and confirmed
by measurement at the relation that contains it. See docs/build-log.md,
"Surprise 17".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from branchlin import parser, pushdown, reconciler
from branchlin.parser import BranchPoint, ParsedStatement
from branchlin.prober import Probe, Prober
from branchlin.pushdown import PushdownResult
from branchlin.reconciler import Verdict
from branchlin.sources import SQLSource

INVESTIGATE = "INVESTIGATE"
EXPLAIN = "EXPLAIN"

#: Hard ceiling on hops, so a cycle or a pathological graph cannot hang a trace.
MAX_HOPS = 40


@dataclass(frozen=True)
class TraceRequest:
    """The four inputs a trace takes.

    ``selector`` is a predicate, not an id: report rows are aggregates and the
    grouping columns are what identify them. ``as_of`` names the logical date
    whose run is under investigation -- it selects *which code* to parse, via
    :mod:`branchlin.pinning`, and is carried here for the record even when the
    caller has already resolved the source.
    """

    relation: str
    selector: str
    column: str
    as_of: str | None = None

    def describe(self) -> str:
        return f"{self.relation}.{self.column} WHERE {self.selector}"


@dataclass(frozen=True)
class BranchObservation:
    """A branch point, and what the traced rows actually did at it."""

    branch: BranchPoint
    #: True/False when measurable, None when it could not be observed.
    taken: bool | None
    rows: int | None
    detail: str
    #: Whether this is worth showing by default in EXPLAIN.
    non_obvious: bool = False

    def describe(self) -> str:
        mark = "*" if self.non_obvious else " "
        return f"{mark} {self.branch.describe()}  --  {self.detail}"


@dataclass
class Hop:
    """One relation the trace visited."""

    index: int
    relation: str
    column: str | None
    selector: str
    probe: Probe | None
    verdict: Verdict
    #: Pushdown results for the relations feeding this one.
    upstream: dict[str, PushdownResult] = field(default_factory=dict)
    #: Which upstream relations the measure actually flows from.
    measure_from: tuple[tuple[str, str], ...] = ()
    #: The (relation, column) the walk continues into -- the fault's path.
    primary: tuple[str, str] | None = None
    #: Contributing inputs that were probed and reconciled but not walked.
    side_inputs: tuple[tuple[str, str], ...] = ()
    step: str | None = None
    origin: str | None = None
    branches: tuple[BranchObservation, ...] = ()
    approximate: bool = False

    @property
    def is_break(self) -> bool:
        return self.verdict.is_break

    @property
    def non_obvious_branches(self) -> tuple[BranchObservation, ...]:
        return tuple(b for b in self.branches if b.non_obvious)


@dataclass
class Trace:
    """The result of a walk."""

    request: TraceRequest
    mode: str
    hops: list[Hop] = field(default_factory=list)
    #: Index into ``hops`` of the first BREAK, if any.
    break_at: int | None = None
    #: Relations reached but not examined, because the trace halted.
    untrusted: tuple[str, ...] = ()
    #: The commit the SQL was read at, when the source was version-pinned.
    source_version: str | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def broke(self) -> bool:
        return self.break_at is not None

    @property
    def break_hop(self) -> Hop | None:
        return self.hops[self.break_at] if self.break_at is not None else None

    @property
    def relations(self) -> tuple[str, ...]:
        return tuple(h.relation for h in self.hops)

    @property
    def hop_count(self) -> int:
        """Hops reported, at relation granularity."""
        return max(len(self.hops) - 1, 0)

    def hop(self, relation: str) -> Hop | None:
        for h in self.hops:
            if h.relation == relation:
                return h
        return None

    # -- rendering ---------------------------------------------------------

    def render(self, *, show_all_branches: bool = False, width: int = 100) -> str:
        out: list[str] = []
        head = f"{self.mode}  {self.request.describe()}"
        out.append(head)
        if self.request.as_of:
            out.append(f"as of {self.request.as_of}"
                       + (f"  code @ {self.source_version}" if self.source_version else ""))
        elif self.source_version:
            out.append(f"code @ {self.source_version}")
        out.append("=" * width)

        for h in self.hops:
            verdict = h.verdict.verdict
            if h.approximate and verdict == reconciler.BALANCED:
                verdict = reconciler.APPROXIMATE
            value = h.probe.value if h.probe else None
            rows = f"{h.probe.rows:,}" if h.probe else "-"
            nulls = (
                f"{h.probe.nulls:,}" if h.probe and h.probe.nulls is not None else "-"
            )
            val = f"{value:,.2f}" if isinstance(value, float) else "-"
            out.append(
                f"{h.index:>2}. {verdict:<12} {h.relation:<26} "
                f"{h.column or '-':<22} value={val:>18}  rows={rows:>9}  nulls={nulls:>7}"
            )
            if h.origin:
                out.append(f"        {h.origin}"
                           + (f"   step: {h.step}" if h.step else ""))
            branches = h.branches if show_all_branches else h.non_obvious_branches
            for b in branches:
                out.append(f"        {b.describe()}")
            if h.is_break:
                out.append("        " + "-" * (width - 8))
                out.append(f"        BREAK: {h.verdict.reason}")
                for c in h.verdict.checks:
                    if c.fatal and not c.passed:
                        out.append(f"        {c.describe()}")
                if h.verdict.blame:
                    out.append("        responsible branch points:")
                    for bp in h.verdict.blame[:4]:
                        out.append(f"          [{bp.step}] {bp.describe()}")

        out.append("=" * width)
        if self.broke:
            bh = self.break_hop
            out.append(
                f"ROOT CAUSE  {bh.relation}"
                + (f"  in CTE '{bh.step}'" if bh.step else "")
                + (f"  ({bh.origin})" if bh.origin else "")
            )
            if self.untrusted:
                out.append(
                    f"UNTRUSTED   {len(self.untrusted)} relation(s) upstream of the "
                    f"break: {', '.join(self.untrusted)}"
                )
        else:
            out.append(f"NO BREAK    complete path to source, {self.hop_count} hops")
        for n in self.notes:
            out.append(f"note: {n}")
        return "\n".join(out)


# ---------------------------------------------------------------------------
# the walk
# ---------------------------------------------------------------------------


class Tracer:
    """Walks a pipeline backwards from a value."""

    def __init__(
        self,
        source: SQLSource,
        prober: Prober,
        *,
        source_version: str | None = None,
    ):
        self.source = source
        self.prober = prober
        self.source_version = source_version

    # -- public API --------------------------------------------------------

    def investigate(
        self, relation: str, selector: str, column: str, as_of: str | None = None
    ) -> Trace:
        """Walk until something does not balance."""
        return self._walk(TraceRequest(relation, selector, column, as_of), INVESTIGATE)

    def explain(
        self, relation: str, selector: str, column: str, as_of: str | None = None
    ) -> Trace:
        """Walk all the way to source, annotating branches."""
        return self._walk(TraceRequest(relation, selector, column, as_of), EXPLAIN)

    # -- internals ---------------------------------------------------------

    def _walk(self, request: TraceRequest, mode: str) -> Trace:
        """Follow the measure upstream, one relation at a time.

        The walk is linear, not a breadth-first sweep of the dependency graph.
        At each hop it continues into the *primary* input -- the one the fault
        is travelling through (see :meth:`_choose_primary`) -- and records the
        other contributing inputs as side inputs, probed and reconciled but not
        walked. That is what makes the reported path the causal one: ten hops
        along the amount, rather than every relation reachable from the report.
        """
        trace = Trace(request=request, mode=mode, source_version=self.source_version)

        relation: str | None = request.relation
        column: str | None = request.column
        selector = request.selector
        approximate = False
        visited: set[str] = set()
        index = 0

        while relation is not None and index < MAX_HOPS:
            if relation in visited:
                trace.notes.append(f"stopped: revisited {relation}")
                break
            visited.add(relation)

            hop = self._examine(relation, column, selector, index, approximate)
            trace.hops.append(hop)
            index += 1

            if hop.is_break:
                trace.break_at = len(trace.hops) - 1
                if mode == INVESTIGATE:
                    trace.untrusted = self._upstream_closure(hop, visited)
                    break

            nxt = hop.primary
            if nxt is None:
                break
            relation, column = nxt
            pushed = hop.upstream.get(relation)
            selector = pushed.selector if pushed else ""
            approximate = (not pushed.exact) if pushed else True

        if index >= MAX_HOPS:
            trace.notes.append(f"stopped at the {MAX_HOPS}-hop ceiling")
        return trace

    def _upstream_closure(self, hop: Hop, visited: set[str]) -> tuple[str, ...]:
        """Every relation upstream of a break, transitively.

        Not "wrong" -- unexaminable. The trace would have to pass through the
        broken logic to reach any of these, so their evidence cannot be relied
        upon either way.
        """
        out: list[str] = []
        queue = [rel for rel, _col in hop.measure_from]
        queue.extend(
            rel for rel in self.source.upstream(hop.relation) if rel not in queue
        )
        while queue:
            rel = queue.pop(0)
            if rel in visited or rel in out:
                continue
            out.append(rel)
            for up in self.source.upstream(rel):
                if up not in out and up not in visited:
                    queue.append(up)
        return tuple(out)

    def _choose_primary(
        self,
        origins: tuple[tuple[str, str], ...],
        inputs: dict[tuple[str, str], Probe],
        probe: Probe,
        parsed: ParsedStatement,
        column: str | None,
    ) -> tuple[tuple[str, str] | None, tuple[tuple[str, str], ...]]:
        """Pick the input to continue into, and return the rest as side inputs.

        **Follow the nulls when there are nulls to follow.** If the output
        carries NULLs and a contributing input carries them too, the fault came
        in through that input and that is the hop worth walking. This is
        evidence-driven rather than a guess at which column "is" the measure --
        and it is what makes the walk land on the documented causal path
        instead of wandering into ``net_quantity``.

        With no nulls anywhere (a healthy EXPLAIN), fall back to the primary
        FROM lineage of the step that computes the column, preferring an input
        column of the same name.
        """
        produced = [
            (rel, col) for rel, col in origins if not self.source.is_source_relation(rel)
        ]
        if not produced:
            # Nothing produced by this pipeline feeds the measure any further,
            # so the next stop is a raw landing table. Walk into it rather than
            # stopping one short: "the trace reached source" is a conclusion,
            # and it should appear in the report as a hop like any other.
            if origins:
                return origins[0], tuple(origins[1:])
            return None, ()

        candidates = produced

        # When the output carries NULLs, the fault arrived through an input that
        # carries them too. Restrict to those -- unless none do, in which case
        # this hop introduced them and is about to be reported as the BREAK.
        if probe.column is not None and probe.nulls:
            with_nulls = [
                rc for rc in candidates
                if (p := inputs.get(rc)) is not None and p.nulls
            ]
            if with_nulls:
                candidates = with_nulls

        # Continuity of the measure beats volume of nulls. A hop like
        # SUM(mtm_pnl_usd * cp_weight) draws on the weight's whole lineage as
        # well as the amount's, and the weight can carry more NULLs than the
        # amount does; following it would skip four relations and report a path
        # the money never took. An input column of the same name is the
        # measure continuing under its own name, so it wins outright.
        same_name = [rc for rc in candidates if rc[1] == column]
        if same_name:
            chosen = same_name[0]
        else:
            # Otherwise prefer the step's own FROM lineage -- the row's spine,
            # rather than a relation joined in to decorate it.
            spine = pushdown.primary_source_relation(
                parsed, pushdown.computing_step(parsed, column) if column else None
            )
            on_spine = [rc for rc in candidates if rc[0] == spine]
            pool = on_spine or candidates
            # Among equally plausible inputs, prefer the one whose name shares
            # the most with the measure being traced. In this pipeline that
            # keeps a money trace on money: mtm_pnl_usd continues into
            # net_amount_usd rather than net_quantity, which shares no suffix.
            # Without it a healthy EXPLAIN silently changes what it is
            # reporting the value *of* halfway down the path.
            chosen = max(
                pool,
                key=lambda rc: (
                    _common_suffix(rc[1], column or ""),
                    (inputs[rc].nulls or 0) if rc in inputs else 0,
                ),
            )

        side = tuple(rc for rc in origins if rc != chosen)
        return chosen, side

    def _examine(
        self,
        relation: str,
        column: str | None,
        selector: str,
        index: int,
        approximate: bool,
    ) -> Hop:
        """Probe one relation, push its selector, reconcile, observe branches."""
        statement = self.source.get(relation)

        probe = None
        try:
            probe = self.prober.probe(relation, column, selector or None)
        except Exception as exc:  # a missing column or relation is evidence too
            return Hop(
                index=index,
                relation=relation,
                column=column,
                selector=selector,
                probe=None,
                verdict=Verdict(
                    verdict=reconciler.BREAK,
                    reason=f"could not probe: {exc}",
                ),
                approximate=approximate,
            )

        # A relation nothing in this source produces is a raw input: the walk
        # has reached source and there is nothing further to reconcile.
        if statement is None:
            return Hop(
                index=index,
                relation=relation,
                column=column,
                selector=selector,
                probe=probe,
                verdict=Verdict(
                    verdict=reconciler.BALANCED,
                    reason="source relation -- nothing upstream in this pipeline",
                ),
                approximate=approximate,
            )

        parsed = self.source.parse(relation)

        origins: tuple[pushdown.MeasureOrigin, ...] = ()
        step = None
        if column is not None:
            origins = pushdown.measure_inputs(parsed, column)
            step = pushdown.computing_step(parsed, column)

        # Push the selector to every declared input, then probe the ones the
        # measure actually flows from.
        pushed = pushdown.push(parsed, selector or None)
        measure_from = tuple(
            (o.relation, o.column) for o in origins
        )

        inputs: dict[tuple[str, str], Probe] = {}
        for rel, col in measure_from:
            p = pushed.get(rel)
            sel = p.selector if p else ""
            try:
                inputs[(rel, col)] = self.prober.probe(rel, col, sel or None)
            except Exception:
                # An unprobeable input is not evidence of a break here; the
                # reconciler simply has less to work with.
                continue

        aggregates = any(
            s.aggregates for s in parsed.steps if s.name == (step or "")
        ) or any(s.aggregates for s in parsed.steps)

        widened = approximate or any(
            not p.exact for rel, p in pushed.items() if rel in dict(measure_from)
        )

        verdict = reconciler.reconcile(
            output=probe,
            inputs=inputs,
            parsed=parsed,
            column=column,
            approximate=widened,
            aggregates=aggregates,
        )

        branches = self._observe_branches(relation, parsed, selector, inputs, probe)

        primary, side = self._choose_primary(
            measure_from, inputs, probe, parsed, column
        )

        return Hop(
            index=index,
            relation=relation,
            column=column,
            selector=selector,
            probe=probe,
            verdict=verdict,
            upstream=pushed,
            measure_from=measure_from,
            primary=primary,
            side_inputs=side,
            step=verdict.step or step,
            origin=statement.origin,
            branches=branches,
            approximate=widened,
        )

    # -- branch observation (what EXPLAIN reports) -------------------------

    def _observe_branches(
        self,
        relation: str,
        parsed: ParsedStatement,
        selector: str,
        inputs: dict[tuple[str, str], Probe],
        probe: Probe,
    ) -> tuple[BranchObservation, ...]:
        """Which branch the traced rows actually took, where it is measurable.

        A CASE whose result is materialised as a column can be observed
        directly: probe the distinct values of that column under the selector
        and match them against the arms. An outer join is observed through the
        nulls it left behind. Branch points inside a CTE whose result is never
        projected cannot be observed without re-executing the CTE, and are
        reported as unobserved rather than guessed at.

        "Non-obvious" -- what EXPLAIN shows by default -- means an ELSE arm was
        taken, or an outer join null-extended. Near-miss predicates ("this
        row was one day inside the window") are not detected in Phase 1; doing
        it properly needs CTE-level re-execution.
        """
        observations: list[BranchObservation] = []
        available = set()
        try:
            available = set(self.prober.columns(relation))
        except Exception:
            pass

        # CASE arms, grouped by the column they feed.
        case_columns: dict[str, list[BranchPoint]] = {}
        for bp in parsed.of_kind(parser.CASE_WHEN, parser.CASE_ELSE):
            if bp.column and bp.column in available:
                case_columns.setdefault(bp.column, []).append(bp)

        for col, arms in case_columns.items():
            try:
                observed = self.prober.distinct_values(
                    relation, col, selector or None, limit=20
                )
            except Exception:
                continue
            counts = {str(v): n for v, n in observed}
            for bp in arms:
                literal = _unquote(bp.outcome)
                rows = counts.get(literal)
                taken = rows is not None and rows > 0
                observations.append(BranchObservation(
                    branch=bp,
                    taken=taken,
                    rows=rows,
                    detail=(
                        f"{col} = {literal!r} for {rows:,} rows"
                        if taken else f"{col} never took {literal!r} for these rows"
                    ),
                    non_obvious=bool(taken and bp.kind == parser.CASE_ELSE),
                ))

        # Outer joins: observed through null extension in the columns they add.
        for bp in parsed.of_kind(parser.JOIN):
            if not bp.can_null_extend:
                continue
            joined = (bp.joined_relation or "").split(" ")[0]
            nulls = probe.nulls if probe.column else None
            detail = f"outer join on {joined}"
            non_obvious = False
            if nulls:
                detail = (
                    f"outer join on {joined} left {nulls:,} NULL in "
                    f"{probe.column} -- rows that found no match"
                )
                non_obvious = True
            observations.append(BranchObservation(
                branch=bp,
                taken=None if not nulls else True,
                rows=nulls,
                detail=detail,
                non_obvious=non_obvious,
            ))

        return tuple(observations)


def _common_suffix(a: str, b: str) -> int:
    """Length of the longest shared trailing substring of two column names.

    A cheap proxy for "same kind of measure": in this pipeline every
    currency-denominated column ends ``_usd``, every count ends ``_count``.
    """
    n = 0
    while n < min(len(a), len(b)) and a[-1 - n] == b[-1 - n]:
        n += 1
    return n


def _unquote(value: str | None) -> str:
    if value is None:
        return ""
    v = value.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
        return v[1:-1]
    return v
