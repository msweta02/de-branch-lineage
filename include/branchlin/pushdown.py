"""Rewriting a selector backwards through a statement.

The mechanism the whole trace depends on. Given a selector on a relation and
the SQL that produced it, derive the equivalent selector on each upstream
relation.

Report rows are aggregates with no primary key -- the grouping columns are what
identify them -- so the trace cannot carry an id backwards. It carries a
predicate, and rewrites it at every hop.

**Aggregation is the easy direction, and it is exploited.** A
``GROUP BY (business_date, book_code, ...)`` means a selector on those columns
is *exactly* equivalent upstream: every input row contributing to the selected
output row satisfies the same predicate. The only work is following renames --
``t.trade_date AS business_date`` in ``03_positions/01_aggregate_by_book`` is
the rename that matters most in this pipeline, and it happens inside a CTE, one
level below the GROUP BY.

**Where pushdown is not exact, it widens rather than fails.** A slightly too
broad selector still localises a break; a failed trace does not. Three cases
widen, each recorded so the hop can be reported APPROXIMATE:

* a selector on a column produced by an **aggregate** -- ``SUM(x) = 5`` cannot
  be pushed to the rows that were summed.
* a selector on a **window function** result -- ``RANK() OVER (...) = 1`` is not
  invertible either.
* a selector on a **CASE** result -- the arms are recoverable but the value is
  not invertible in general.

Dropping a conjunct only ever widens the row set, never narrows it, so a break
inside the true row set is still inside the widened one.

**The widening is currently coarser than it needs to be.** All three cases drop
the conjunct outright, which widens to the whole relation. Three refinements
are specified and not implemented:

* a **window** selector could widen to the window's PARTITION BY keys rather
  than to everything, since only rows in the same partition participated.
* a **non-equi join** could be re-executed rather than widened away.
* a **QUALIFY** dedupe could return all candidate rows and mark which survived,
  rather than dropping the predicate.

Each narrows the widened set without risking a false exoneration, so they are
improvements in precision, not in correctness. The engine is sound without
them -- a too-broad selector still contains the break -- but a trace through a
window-heavy pipeline will do more work than it needs to. See
docs/build-log.md, Phase 1 "Still open".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import sqlglot
from sqlglot import exp

from branchlin.parser import DIALECT, FINAL_STEP, ParsedStatement

# Why a conjunct could not be pushed exactly.
AGGREGATE = "aggregate"
WINDOW = "window"
CASE = "case"
UNKNOWN = "unknown-column"
MIXED = "spans-multiple-relations"
LITERAL = "not-a-column"


@dataclass(frozen=True)
class Resolved:
    """Where an output column came from, one hop up."""

    relation: str
    column: str
    exact: bool
    reason: str | None = None
    #: Steps the resolution passed through, innermost last.
    via: tuple[str, ...] = ()


@dataclass(frozen=True)
class PushdownResult:
    """A selector rewritten for one upstream relation."""

    relation: str
    selector: str
    exact: bool
    #: (downstream column, upstream column) pairs actually pushed.
    mapped: tuple[tuple[str, str], ...] = ()
    #: Conjuncts that could not be pushed, with the reason.
    dropped: tuple[tuple[str, str], ...] = ()
    notes: tuple[str, ...] = ()

    @property
    def widened(self) -> bool:
        return bool(self.dropped)

    @property
    def unrestricted(self) -> bool:
        """True when nothing at all could be pushed -- the whole relation."""
        return not self.selector

    def describe(self) -> str:
        sel = self.selector or "(unrestricted)"
        flag = "" if self.exact else "  ~approximate"
        return f"{self.relation}: {sel}{flag}"


# ---------------------------------------------------------------------------
# statement structure helpers
# ---------------------------------------------------------------------------


def _select_of(node: exp.Expression) -> exp.Select | None:
    if isinstance(node, exp.Select):
        return node
    return node.find(exp.Select)


def _step_selects(parsed: ParsedStatement) -> dict[str, exp.Select]:
    """Map step name -> the SELECT that defines it."""
    out: dict[str, exp.Select] = {}
    tree = parsed.expression
    for cte in tree.find_all(exp.CTE):
        sel = _select_of(cte.this)
        if sel is not None:
            out[cte.alias_or_name] = sel
    # The outermost SELECT is the one whose ancestors include no CTE.
    for sel in tree.find_all(exp.Select):
        if not any(isinstance(a, exp.CTE) for a in _ancestors(sel)):
            out[FINAL_STEP] = sel
            break
    return out


def _ancestors(node: exp.Expression) -> Iterable[exp.Expression]:
    cur = node.parent
    while cur is not None:
        yield cur
        cur = cur.parent


def _sources(select: exp.Select) -> dict[str, str]:
    """Map alias (and bare name) -> relation/step name, for one SELECT."""
    out: dict[str, str] = {}
    frm = select.args.get("from_") or select.args.get("from")
    nodes = []
    if frm is not None:
        nodes.extend(frm.find_all(exp.Table))
    for join in select.args.get("joins") or []:
        nodes.extend(join.find_all(exp.Table))
    for table in nodes:
        name = table.sql(dialect=DIALECT).split(" ")[0]
        alias = table.alias
        if alias:
            out[alias] = name
        out.setdefault(name, name)
        # allow lookup by bare table name too, e.g. "fx_rates" for ref.fx_rates
        if "." in name:
            out.setdefault(name.split(".")[-1], name)
    return out


def _primary_source(select: exp.Select) -> str | None:
    """The relation in FROM -- what a bare ``*`` propagates from."""
    frm = select.args.get("from_") or select.args.get("from")
    if frm is None:
        return None
    table = frm.find(exp.Table)
    if table is None:
        return None
    return table.sql(dialect=DIALECT).split(" ")[0]


def _projections(select: exp.Select) -> dict[str, exp.Expression]:
    """Map output column name -> the expression producing it."""
    out: dict[str, exp.Expression] = {}
    for proj in select.expressions:
        if isinstance(proj, exp.Star):
            continue
        if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
            continue
        name = proj.alias_or_name
        if not name:
            continue
        inner = proj.this if isinstance(proj, exp.Alias) else proj
        out[name] = inner
    return out


def _stars(select: exp.Select) -> tuple[str, ...]:
    """Qualifiers of any star projections: ``*`` -> "", ``e.*`` -> "e"."""
    found: list[str] = []
    for proj in select.expressions:
        if isinstance(proj, exp.Star):
            found.append("")
        elif isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
            found.append(proj.table or "")
    return tuple(found)


def _group_keys(select: exp.Select) -> set[str]:
    group = select.args.get("group")
    if group is None:
        return set()
    keys = set()
    for e in group.expressions:
        if isinstance(e, exp.Column):
            keys.add(e.name)
        else:
            keys.add(e.sql(dialect=DIALECT))
    return keys


def _classify(expr: exp.Expression) -> tuple[bool, str | None]:
    """Can an equality on this expression's value be pushed to its inputs?"""
    if isinstance(expr, exp.Window):
        return False, WINDOW
    if isinstance(expr, exp.Case):
        return False, CASE
    if isinstance(expr, exp.AggFunc):
        return False, AGGREGATE
    if expr.find(exp.Window) is not None:
        return False, WINDOW
    if expr.find(exp.AggFunc) is not None:
        return False, AGGREGATE
    if isinstance(expr, exp.Column):
        return True, None
    if expr.find(exp.Case) is not None:
        return False, CASE
    if expr.find(exp.Column) is None:
        return False, LITERAL
    # Arithmetic over columns: the value is not invertible, but if it is a bare
    # cast or alias of a single column we can still follow it.
    cols = list(expr.find_all(exp.Column))
    if len(cols) == 1 and isinstance(expr, (exp.Cast, exp.Paren, exp.Coalesce)):
        return True, None
    return False, UNKNOWN


# ---------------------------------------------------------------------------
# column resolution
# ---------------------------------------------------------------------------


def resolve_column(
    parsed: ParsedStatement,
    column: str,
    *,
    step: str = FINAL_STEP,
    _seen: tuple[str, ...] = (),
) -> Resolved | None:
    """Follow ``column`` from ``step`` back to the relation that supplied it.

    Returns None when the column is not visible at all. Otherwise the relation
    is one of the statement's declared inputs, and ``exact`` says whether an
    equality on the column means the same thing there.
    """
    selects = _step_selects(parsed)
    select = selects.get(step)
    if select is None:
        return None

    via = _seen + (step,)
    projections = _projections(select)
    sources = _sources(select)

    if column in projections:
        expr = projections[column]
        ok, reason = _classify(expr)
        if not ok:
            # Value is not invertible here. Report the step so the caller can
            # widen and mark the hop approximate.
            rel = _primary_source(select) or step
            return Resolved(rel, column, exact=False, reason=reason, via=via)
        col = expr if isinstance(expr, exp.Column) else expr.find(exp.Column)
        next_name = col.name
        qualifier = col.table
        target = sources.get(qualifier) if qualifier else _primary_source(select)
        if target is None:
            target = _primary_source(select)
        if target in selects:  # an earlier CTE in this same statement
            deeper = resolve_column(parsed, next_name, step=target, _seen=via)
            if deeper is None:
                return Resolved(target, next_name, exact=True, via=via)
            return deeper
        if target is None:
            return Resolved(step, next_name, exact=False, reason=UNKNOWN, via=via)
        return Resolved(target, next_name, exact=True, via=via)

    # Not projected explicitly -- a star may be carrying it through unchanged.
    stars = _stars(select)
    if stars:
        for qualifier in stars:
            target = sources.get(qualifier) if qualifier else _primary_source(select)
            if target is None:
                continue
            if target in selects:
                deeper = resolve_column(parsed, column, step=target, _seen=via)
                if deeper is not None:
                    return deeper
            else:
                return Resolved(target, column, exact=True, via=via)
    return None


# ---------------------------------------------------------------------------
# selector rewriting
# ---------------------------------------------------------------------------


def parse_selector(selector: str) -> exp.Expression:
    """Parse a bare predicate into an expression tree."""
    wrapped = sqlglot.parse_one(f"SELECT 1 WHERE {selector}", read=DIALECT)
    where = wrapped.find(exp.Where)
    if where is None:
        raise ValueError(f"could not parse selector: {selector!r}")
    return where.this


def conjuncts(selector: str) -> tuple[exp.Expression, ...]:
    """Split a selector into independently-pushable AND conjuncts."""
    pred = parse_selector(selector)
    if isinstance(pred, exp.And):
        return tuple(pred.flatten())
    return (pred,)


def push(
    parsed: ParsedStatement,
    selector: str | None,
    *,
    relations: Iterable[str] | None = None,
) -> dict[str, PushdownResult]:
    """Rewrite ``selector`` for every upstream relation of ``parsed``.

    ``relations`` restricts the result; by default every declared dependency
    gets an entry, so a caller can walk whichever inputs it cares about.
    """
    wanted = tuple(relations) if relations is not None else parsed.depends
    if not selector:
        return {
            rel: PushdownResult(rel, "", exact=True, notes=("no selector to push",))
            for rel in wanted
        }

    # Resolve every column mentioned in the selector, once.
    per_relation: dict[str, list[str]] = {rel: [] for rel in wanted}
    mapped: dict[str, list[tuple[str, str]]] = {rel: [] for rel in wanted}
    dropped: dict[str, list[tuple[str, str]]] = {rel: [] for rel in wanted}
    notes: dict[str, list[str]] = {rel: [] for rel in wanted}

    for conj in conjuncts(selector):
        rendered = conj.sql(dialect=DIALECT)
        cols = list(conj.find_all(exp.Column))
        if not cols:
            for rel in wanted:
                dropped[rel].append((rendered, LITERAL))
            continue

        resolutions: dict[str, Resolved] = {}
        failed_reason: str | None = None
        for col in cols:
            res = resolve_column(parsed, col.name)
            if res is None:
                failed_reason = UNKNOWN
                break
            if not res.exact:
                failed_reason = res.reason or UNKNOWN
                break
            resolutions[col.name] = res
        if failed_reason is not None:
            for rel in wanted:
                dropped[rel].append((rendered, failed_reason))
            continue

        targets = {r.relation for r in resolutions.values()}
        if len(targets) > 1:
            for rel in wanted:
                dropped[rel].append((rendered, MIXED))
            continue
        target = targets.pop()
        if target not in per_relation:
            # Resolved to a relation the caller did not ask about (or to an
            # intermediate step). Nothing to push for the wanted relations.
            for rel in wanted:
                dropped[rel].append((rendered, f"resolves to {target}"))
            continue

        rewritten = _rewrite(conj, resolutions)
        per_relation[target].append(rewritten)
        for down, res in resolutions.items():
            if (down, res.column) not in mapped[target]:
                mapped[target].append((down, res.column))

    out: dict[str, PushdownResult] = {}
    for rel in wanted:
        pushed = per_relation[rel]
        drops = dropped[rel]
        # A conjunct dropped because it belongs to a *different* upstream
        # relation is not a widening of this one, so it does not make this hop
        # approximate. Only genuine inversion failures do.
        real_drops = [d for d in drops if not d[1].startswith("resolves to")]
        sel = " AND ".join(pushed)
        exact = bool(pushed) and not real_drops
        note = list(notes[rel])
        if not pushed:
            note.append("nothing pushable; widened to the whole relation")
        out[rel] = PushdownResult(
            relation=rel,
            selector=sel,
            exact=exact,
            mapped=tuple(mapped[rel]),
            dropped=tuple(real_drops),
            notes=tuple(note),
        )
    return out


def _rewrite(conj: exp.Expression, resolutions: dict[str, Resolved]) -> str:
    """Render a conjunct with its columns renamed to their upstream names."""
    clone = conj.copy()
    for col in clone.find_all(exp.Column):
        res = resolutions.get(col.name)
        if res is None:
            continue
        col.set("this", exp.to_identifier(res.column))
        col.set("table", None)
    return clone.sql(dialect=DIALECT)


# ---------------------------------------------------------------------------
# measure lineage -- what feeds a value, as opposed to what identifies a row
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MeasureOrigin:
    """An upstream column that contributes to an output measure.

    ``aggregate`` is the aggregate applied on the way out (``SUM``, ``COUNT``,
    ...) or None for a row-wise expression. ``step`` is the step in which the
    output value is computed -- which is what localises a break to a CTE rather
    than merely to a file.
    """

    relation: str
    column: str
    aggregate: str | None
    step: str
    expression: str
    #: True when this column is consumed under COALESCE/IFNULL, so it cannot
    #: transmit a NULL downstream however many it holds.
    null_safe: bool = False


def _under_coalesce(col: exp.Expression, root: exp.Expression) -> bool:
    """Is this column reference wrapped in a COALESCE within ``root``?"""
    cur = col
    while cur is not None and cur is not root.parent:
        if isinstance(cur, exp.Coalesce):
            return True
        cur = cur.parent
    return False


def _agg_name(expr: exp.Expression) -> str | None:
    agg = expr if isinstance(expr, exp.AggFunc) else expr.find(exp.AggFunc)
    return type(agg).__name__.upper() if agg is not None else None


def measure_inputs(
    parsed: ParsedStatement,
    column: str,
    *,
    step: str = FINAL_STEP,
) -> tuple[MeasureOrigin, ...]:
    """Every upstream column that contributes to ``column``.

    Unlike :func:`resolve_column`, this follows *through* aggregates and
    arithmetic -- it answers "what values produced this number", which is the
    question the reconciler needs. Column-level lineage stops here; branch
    lineage uses it as a substrate and then asks which branch each row took.
    """
    selects = _step_selects(parsed)
    select = selects.get(step)
    if select is None:
        return ()

    projections = _projections(select)
    sources = _sources(select)

    if column not in projections:
        stars = _stars(select)
        for qualifier in stars:
            target = sources.get(qualifier) if qualifier else _primary_source(select)
            if target is None:
                continue
            if target in selects:
                deeper = measure_inputs(parsed, column, step=target)
                if deeper:
                    return deeper
            else:
                return (MeasureOrigin(target, column, None, step, column),)
        return ()

    expr = projections[column]
    rendered = expr.sql(dialect=DIALECT)
    aggregate = _agg_name(expr)

    cols = list(expr.find_all(exp.Column))
    if not cols:
        return ()

    out: list[MeasureOrigin] = []
    for col in cols:
        if isinstance(col.this, exp.Star):
            continue
        qualifier = col.table
        target = sources.get(qualifier) if qualifier else _primary_source(select)
        if target is None:
            continue
        safe = _under_coalesce(col, expr)
        if target in selects:
            deeper = measure_inputs(parsed, col.name, step=target)
            if deeper:
                for d in deeper:
                    out.append(MeasureOrigin(
                        d.relation, d.column,
                        aggregate or d.aggregate,
                        step if aggregate else d.step,
                        rendered if aggregate else d.expression,
                        null_safe=safe or d.null_safe,
                    ))
                continue
        out.append(MeasureOrigin(
            target, col.name, aggregate, step, rendered, null_safe=safe
        ))

    # De-duplicate while preserving order.
    seen: set[tuple[str, str]] = set()
    unique: list[MeasureOrigin] = []
    for o in out:
        key = (o.relation, o.column)
        if key not in seen:
            seen.add(key)
            unique.append(o)
    return tuple(unique)


def computing_step(
    parsed: ParsedStatement,
    column: str,
    *,
    step: str = FINAL_STEP,
) -> str:
    """The step in which ``column``'s value is actually computed.

    Defined as the innermost step whose projection of ``column`` is something
    other than a bare pass-through of an earlier step's column. For
    ``base_amount`` in ``05_convert_currency`` that is the ``converted`` CTE --
    the step holding the LEFT JOIN that can null-extend it. Reporting is at
    relation granularity; this is what lets a hop be expanded to the CTE that
    broke.
    """
    selects = _step_selects(parsed)
    select = selects.get(step)
    if select is None:
        return step

    projections = _projections(select)
    sources = _sources(select)

    if column in projections:
        expr = projections[column]
        if isinstance(expr, exp.Column) and not isinstance(expr.this, exp.Star):
            qualifier = expr.table
            target = sources.get(qualifier) if qualifier else _primary_source(select)
            if target in selects:
                return computing_step(parsed, expr.name, step=target)
        # A non-trivial expression, or a read straight off a base relation:
        # this is where the value comes into being.
        return step

    for qualifier in _stars(select):
        target = sources.get(qualifier) if qualifier else _primary_source(select)
        if target is None:
            continue
        if target in selects:
            return computing_step(parsed, column, step=target)
    return step


def primary_source_relation(
    parsed: ParsedStatement,
    step: str | None = None,
) -> str | None:
    """Follow a step's FROM chain down to the relation it is built on.

    The "spine" of a step: the relation whose rows it is shaped by, as opposed
    to relations joined in to decorate those rows. Used to choose which input a
    trace should continue into when the measure changes name across the hop.
    """
    selects = _step_selects(parsed)
    current = step or FINAL_STEP
    seen: set[str] = set()
    while current in selects and current not in seen:
        seen.add(current)
        nxt = _primary_source(selects[current])
        if nxt is None:
            return None
        if nxt not in selects:
            return nxt
        current = nxt
    return None
