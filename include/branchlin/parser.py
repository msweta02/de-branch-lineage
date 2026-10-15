"""SQL branch extraction via sqlglot.

Parses a single SQL statement into the set of *branch points* it contains:

* ``CASE`` expressions       -- one branch per WHEN arm, plus the ELSE arm.
* ``WHERE`` / ``HAVING``     -- one branch per top-level conjunct, since each
                                conjunct can independently exclude a row.
* ``JOIN``                   -- one branch per join, distinguishing matched
                                from unmatched for outer joins (the null-
                                extension case that column-level lineage
                                cannot see).
* ``QUALIFY`` / window filters -- dedupe predicates, which drop rows without
                                any WHERE clause being involved.

The parser is deliberately agnostic about where the SQL came from. It
consumes whatever :mod:`branchlin.sources` hands it -- raw ``.sql`` files or
dbt's compiled output -- and never reaches for a filesystem itself.

Every branch point is attributed to the *step* that contains it: the name of
the enclosing CTE, or :data:`FINAL_STEP` for the outermost SELECT. That is the
granularity the tracer walks (30 steps across the BUG A causal path); hops are
reported at relation granularity. See docs/technical-reference.md, "Branch
points on the BUG A path".
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Iterator

import sqlglot
from sqlglot import exp

DIALECT = "duckdb"

#: Step name used for branch points in the outermost SELECT, outside any CTE.
FINAL_STEP = "__final__"

# Branch point kinds.
CASE_WHEN = "case_when"
CASE_ELSE = "case_else"
WHERE = "where"
HAVING = "having"
JOIN = "join"
QUALIFY = "qualify"

#: The three kinds whose sum is the "69 branch points" figure quoted in the
#: README and in docs/technical-reference.md, "Branch points on the BUG A
#: path". QUALIFY is deliberately excluded: the original count was produced
#: before the parser looked for it. See docs/build-log.md, "Surprise 14".
DOCUMENTED_KINDS = (CASE_WHEN, CASE_ELSE, WHERE, HAVING, JOIN)

_TARGET_RE = re.compile(r"^--\s*target:\s*(\S+)", re.MULTILINE)
_DEPENDS_RE = re.compile(r"^--\s*depends:\s*(.+)$", re.MULTILINE)

_WS_RE = re.compile(r"\s+")


def _norm(sql: str) -> str:
    """Collapse whitespace so rendered predicates compare and print cleanly."""
    return _WS_RE.sub(" ", sql).strip()


@dataclass(frozen=True)
class BranchPoint:
    """One place where a row's fate depends on a decision.

    ``kind`` is one of the module-level constants. ``step`` is the enclosing
    CTE name or :data:`FINAL_STEP`. ``expression`` is the rendered predicate --
    for a CASE arm the condition, for the ELSE arm an empty string, since an
    ELSE has no condition of its own beyond every WHEN having failed.
    """

    kind: str
    step: str
    expression: str
    #: For CASE arms: the value the arm produces, rendered. None otherwise.
    outcome: str | None = None
    #: For CASE arms: the output column the expression is aliased to, if known.
    column: str | None = None
    #: For joins: LEFT / RIGHT / FULL / INNER / CROSS.
    join_side: str | None = None
    #: True for joins that can produce NULL-extended rows -- the failure mode
    #: BUG A exploits and the one column-level lineage cannot see.
    can_null_extend: bool = False
    #: For joins: the relation being joined in, as written.
    joined_relation: str | None = None

    @property
    def is_case_arm(self) -> bool:
        return self.kind in (CASE_WHEN, CASE_ELSE)

    @property
    def is_filter(self) -> bool:
        return self.kind in (WHERE, HAVING, QUALIFY)

    def describe(self) -> str:
        if self.kind == CASE_WHEN:
            return f"WHEN {self.expression} THEN {self.outcome}"
        if self.kind == CASE_ELSE:
            return f"ELSE {self.outcome}"
        if self.kind == JOIN:
            return f"{self.join_side} JOIN {self.joined_relation} ON {self.expression}"
        return f"{self.kind.upper()} {self.expression}"


@dataclass
class Step:
    """A CTE (or the final SELECT) inside one statement."""

    name: str
    #: Relations and earlier steps this step reads from, as written.
    reads: tuple[str, ...] = ()
    #: GROUP BY keys, rendered. Empty when the step does not aggregate.
    group_by: tuple[str, ...] = ()
    branch_points: tuple[BranchPoint, ...] = ()

    @property
    def aggregates(self) -> bool:
        return bool(self.group_by)


@dataclass
class ParsedStatement:
    """The parse of one ``.sql`` file."""

    target: str | None
    depends: tuple[str, ...]
    steps: tuple[Step, ...]
    branch_points: tuple[BranchPoint, ...]
    expression: exp.Expression = field(repr=False, compare=False, default=None)

    @property
    def cte_steps(self) -> tuple[Step, ...]:
        """Steps excluding the final SELECT -- the "30 CTE-level steps" figure."""
        return tuple(s for s in self.steps if s.name != FINAL_STEP)

    def step(self, name: str) -> Step | None:
        for s in self.steps:
            if s.name == name:
                return s
        return None

    def of_kind(self, *kinds: str) -> tuple[BranchPoint, ...]:
        return tuple(bp for bp in self.branch_points if bp.kind in kinds)

    def counts(self) -> dict[str, int]:
        """Branch points by kind, plus the documented subtotal and true total."""
        out = {k: 0 for k in (CASE_WHEN, CASE_ELSE, WHERE, HAVING, JOIN, QUALIFY)}
        for bp in self.branch_points:
            out[bp.kind] += 1
        out["case_arms"] = out[CASE_WHEN] + out[CASE_ELSE]
        out["documented"] = sum(out[k] for k in DOCUMENTED_KINDS)
        out["total"] = len(self.branch_points)
        out["cte_steps"] = len(self.cte_steps)
        return out


def header(sql_text: str) -> tuple[str | None, tuple[str, ...]]:
    """Pull the declared target relation and inputs out of the header comment.

    The `-- depends:` headers are the single source of truth for dependency
    ordering, matching ``include/runner.py``. Deliberately a plain regex over
    the text rather than a sqlglot walk: the header is a comment, and comment
    attachment is not something to depend on.
    """
    m = _TARGET_RE.search(sql_text)
    target = m.group(1) if m else None
    d = _DEPENDS_RE.search(sql_text)
    depends = tuple(x.strip() for x in d.group(1).split(",")) if d else ()
    return target, depends


def _owning_step(node: exp.Expression, cte_ids: dict[int, str]) -> str:
    """Walk up from ``node`` to the innermost enclosing CTE, else FINAL_STEP."""
    cur = node
    while cur is not None:
        owner = cte_ids.get(id(cur))
        if owner is not None:
            return owner
        cur = cur.parent
    return FINAL_STEP


def _aliased_column(node: exp.Expression) -> str | None:
    """The output column a CASE expression feeds, if it is directly aliased."""
    parent = node.parent
    if isinstance(parent, exp.Alias):
        return parent.alias_or_name
    return None


def _conjuncts(predicate: exp.Expression | None) -> list[exp.Expression]:
    """Split a predicate into top-level AND conjuncts.

    Each conjunct can exclude a row on its own, so each is its own branch
    point. ORs are left whole -- an OR is one decision with internal
    structure, not two independent filters.
    """
    if predicate is None:
        return []
    if isinstance(predicate, exp.And):
        return list(predicate.flatten())
    return [predicate]


def _relation_name(node: exp.Expression | None) -> str | None:
    if node is None:
        return None
    if isinstance(node, exp.Table):
        return node.sql(dialect=DIALECT)
    return _norm(node.sql(dialect=DIALECT))[:80]


def _select_scopes(tree: exp.Expression) -> Iterator[exp.Select]:
    yield from tree.find_all(exp.Select)


def parse(sql_text: str) -> ParsedStatement:
    """Parse one SQL statement into its steps and branch points."""
    target, depends = header(sql_text)
    tree = sqlglot.parse_one(sql_text, read=DIALECT)

    # Map every node inside a CTE to that CTE's alias, so a branch point can be
    # attributed to the step containing it. Innermost wins, which _owning_step
    # gets for free by walking up from the node.
    cte_ids: dict[int, str] = {}
    ctes: list[exp.CTE] = list(tree.find_all(exp.CTE))
    for cte in ctes:
        cte_ids[id(cte)] = cte.alias_or_name

    branch_points: list[BranchPoint] = []

    def add(bp: BranchPoint) -> None:
        branch_points.append(bp)

    # --- CASE arms: one per WHEN, plus the ELSE ---------------------------
    for case in tree.find_all(exp.Case):
        step = _owning_step(case, cte_ids)
        column = _aliased_column(case)
        for arm in case.args.get("ifs") or []:
            add(BranchPoint(
                kind=CASE_WHEN,
                step=step,
                expression=_norm(arm.this.sql(dialect=DIALECT)),
                outcome=_norm(arm.args["true"].sql(dialect=DIALECT)),
                column=column,
            ))
        default = case.args.get("default")
        if default is not None:
            add(BranchPoint(
                kind=CASE_ELSE,
                step=step,
                expression="",
                outcome=_norm(default.sql(dialect=DIALECT)),
                column=column,
            ))

    # --- WHERE / HAVING: one per top-level conjunct -----------------------
    for clause, kind in ((exp.Where, WHERE), (exp.Having, HAVING)):
        for node in tree.find_all(clause):
            step = _owning_step(node, cte_ids)
            for conj in _conjuncts(node.this):
                add(BranchPoint(
                    kind=kind,
                    step=step,
                    expression=_norm(conj.sql(dialect=DIALECT)),
                ))

    # --- JOINs: one per join that carries a condition ---------------------
    # A CROSS JOIN has no ON and cannot exclude or null-extend a row on a
    # condition, so it is not a branch point. It is still recorded as a read.
    for join in tree.find_all(exp.Join):
        on = join.args.get("on")
        if on is None:
            continue
        side = (join.side or join.kind or "INNER").upper()
        add(BranchPoint(
            kind=JOIN,
            step=_owning_step(join, cte_ids),
            expression=_norm(on.sql(dialect=DIALECT)),
            join_side=side,
            can_null_extend=side in ("LEFT", "RIGHT", "FULL"),
            joined_relation=_relation_name(join.this),
        ))

    # --- QUALIFY: window-function dedupe, drops rows with no WHERE --------
    for node in tree.find_all(exp.Qualify):
        step = _owning_step(node, cte_ids)
        for conj in _conjuncts(node.this):
            add(BranchPoint(
                kind=QUALIFY,
                step=step,
                expression=_norm(conj.sql(dialect=DIALECT)),
            ))

    steps = _build_steps(tree, ctes, cte_ids, branch_points)
    return ParsedStatement(
        target=target,
        depends=depends,
        steps=steps,
        branch_points=tuple(branch_points),
        expression=tree,
    )


def _step_reads(scope: exp.Expression) -> tuple[str, ...]:
    """Relations and earlier steps a step reads, in FROM/JOIN order."""
    reads: list[str] = []
    select = scope if isinstance(scope, exp.Select) else scope.find(exp.Select)
    if select is None:
        return ()
    # sqlglot 30 renamed this arg key from "from" to "from_", and args.get on
    # the old name returns None rather than raising -- a silently empty read
    # list. Accept both. See docs/build-log.md, "Surprise 15".
    frm = select.args.get("from_") or select.args.get("from")
    if frm is not None:
        for table in frm.find_all(exp.Table):
            name = table.sql(dialect=DIALECT).split(" ")[0]
            if name not in reads:
                reads.append(name)
    for join in select.args.get("joins") or []:
        for table in join.find_all(exp.Table):
            name = table.sql(dialect=DIALECT).split(" ")[0]
            if name not in reads:
                reads.append(name)
    return tuple(reads)


def _group_keys(scope: exp.Expression) -> tuple[str, ...]:
    select = scope if isinstance(scope, exp.Select) else scope.find(exp.Select)
    if select is None:
        return ()
    group = select.args.get("group")
    if group is None:
        return ()
    return tuple(_norm(e.sql(dialect=DIALECT)) for e in group.expressions)


def _build_steps(
    tree: exp.Expression,
    ctes: Iterable[exp.CTE],
    cte_ids: dict[int, str],
    branch_points: Iterable[BranchPoint],
) -> tuple[Step, ...]:
    by_step: dict[str, list[BranchPoint]] = {}
    for bp in branch_points:
        by_step.setdefault(bp.step, []).append(bp)

    steps: list[Step] = []
    for cte in ctes:
        name = cte.alias_or_name
        steps.append(Step(
            name=name,
            reads=_step_reads(cte.this),
            group_by=_group_keys(cte.this),
            branch_points=tuple(by_step.get(name, ())),
        ))

    # The outermost SELECT: the one that is not inside a CTE.
    final = None
    for select in tree.find_all(exp.Select):
        if _owning_step(select, cte_ids) == FINAL_STEP:
            final = select
            break
    steps.append(Step(
        name=FINAL_STEP,
        reads=_step_reads(final) if final is not None else (),
        group_by=_group_keys(final) if final is not None else (),
        branch_points=tuple(by_step.get(FINAL_STEP, ())),
    ))
    return tuple(steps)


def parse_file(path) -> ParsedStatement:
    """Convenience wrapper -- read a path and parse it."""
    from pathlib import Path

    return parse(Path(path).read_text())
