"""The mechanics of putting a wrong published figure right.

DIAGNOSE answers "which branch broke this". REMEDIATE answers "what now", and
the answer has an order to it: stop publishing, work out what is affected,
recompute it, *ask a human* before telling anyone outside, and lift the embargo
whether or not any of that succeeded.

Everything here is a plain function over a DuckDB connection and a
:class:`~branchlin.sources.SQLSource`. The Airflow DAG in
``dags/remediate.py`` is a thin wrapper: setup, a mapped recompute, an approval
gate and a teardown. Keeping the logic out of the DAG is what makes the failure
path testable without standing up a scheduler.

**The embargo is the part that has to be right.** It is a control table, not a
lock: observable with a SELECT, so a human can answer "is this figure safe to
publish" at any point, including while a remediation is halfway through or has
just failed. It is set by a setup task and lifted by a teardown task, so it is
lifted on success *or* failure -- a remediation that dies mid-recompute must not
leave the warehouse permanently unpublishable, and that is exactly the property
a script cannot give you and an orchestrator can.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Sequence

from branchlin.sources import SQLSource

#: Control tables. All in `meta`, alongside the pipeline's own flags.
EMBARGO_TABLE = "meta.publication_embargo"
NOTIFY_TABLE = "meta.external_notifications"
RECOMPUTE_TABLE = "meta.remediation_log"

#: Schema holding the relations a human actually reads.
REPORT_SCHEMA = "rpt"


class RemediationError(RuntimeError):
    """A remediation step could not be completed."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_tables(con, *, strict: bool = True) -> bool:
    """Create the control tables if they are not there yet.

    Idempotent, and safe to call from every task -- a remediation may be the
    first thing that ever touches these tables, and the teardown must not fail
    merely because the setup did not get far enough to create them.

    ``strict=False`` is for the read paths. Answering "is this figure safe to
    publish" must work over a **read-only** connection: that is how a human or a
    monitoring query would ask, and DuckDB refuses DDL on a read-only handle
    (`Cannot execute statement of type "CREATE" ... attached in read-only
    mode`). Under `strict=False` a failure to create simply means there is
    nothing to read, which the readers below treat as "nothing embargoed".
    """
    try:
        _create_tables(con)
    except Exception:
        if strict:
            raise
        return False
    return True


def _create_tables(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS meta")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {EMBARGO_TABLE} (
            relation      VARCHAR NOT NULL,
            embargoed     BOOLEAN NOT NULL,
            reason        VARCHAR,
            run_id        VARCHAR,
            set_at        TIMESTAMPTZ,
            lifted_at     TIMESTAMPTZ
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {NOTIFY_TABLE} (
            run_id        VARCHAR,
            sent_at       TIMESTAMPTZ,
            channel       VARCHAR,
            recipient     VARCHAR,
            subject       VARCHAR,
            body          VARCHAR,
            payload       VARCHAR
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {RECOMPUTE_TABLE} (
            run_id        VARCHAR,
            relation      VARCHAR,
            rows_after    BIGINT,
            seconds       DOUBLE,
            recomputed_at TIMESTAMPTZ
        )
    """)


# ---------------------------------------------------------------------------
# embargo
# ---------------------------------------------------------------------------


def set_embargo(
    con,
    relations: Iterable[str],
    *,
    run_id: str,
    reason: str,
) -> tuple[str, ...]:
    """Mark ``relations`` as not-for-publication. Returns what was embargoed."""
    ensure_tables(con)
    marked: list[str] = []
    for relation in relations:
        con.execute(
            f"INSERT INTO {EMBARGO_TABLE} "
            "(relation, embargoed, reason, run_id, set_at, lifted_at) "
            "VALUES (?, TRUE, ?, ?, ?, NULL)",
            [relation, reason, run_id, _now()],
        )
        marked.append(relation)
    return tuple(marked)


def lift_embargo(con, *, run_id: str | None = None) -> tuple[str, ...]:
    """Lift the embargo, returning the relations released.

    Called from a teardown task, so it runs whether the remediation succeeded or
    failed. Scoped to ``run_id`` when given so two concurrent remediations
    cannot release each other's holds; with no run_id it lifts everything, which
    is the manual break-glass case.

    Deliberately tolerant: if the control table does not exist yet, there is
    nothing embargoed and that is a success, not an error. A teardown that
    throws is a teardown that leaves an embargo in place.
    """
    ensure_tables(con)

    scope = "embargoed AND lifted_at IS NULL"
    params: list = []
    if run_id is not None:
        scope += " AND run_id = ?"
        params = [run_id]

    released = tuple(
        r[0] for r in con.execute(
            f"SELECT DISTINCT relation FROM {EMBARGO_TABLE} WHERE {scope} "
            "ORDER BY relation",
            params,
        ).fetchall()
    )
    con.execute(
        f"UPDATE {EMBARGO_TABLE} SET embargoed = FALSE, lifted_at = ? WHERE {scope}",
        [_now()] + params,
    )
    return released


def embargoed_relations(con) -> tuple[str, ...]:
    """Everything currently held back from publication.

    Works over a read-only connection: nothing to publish-block is the same
    answer as no control table at all.
    """
    ensure_tables(con, strict=False)
    try:
        rows = con.execute(
            f"SELECT DISTINCT relation FROM {EMBARGO_TABLE} "
            "WHERE embargoed AND lifted_at IS NULL ORDER BY relation"
        ).fetchall()
    except Exception:
        return ()
    return tuple(r[0] for r in rows)


def is_publishable(con, relation: str) -> bool:
    """Can this figure be published right now? The question the table exists to answer."""
    return relation not in embargoed_relations(con)


# ---------------------------------------------------------------------------
# blast radius
# ---------------------------------------------------------------------------


def blast_radius(
    source: SQLSource,
    break_relation: str,
    *,
    include_break: bool = False,
) -> tuple[str, ...]:
    """Everything downstream of the break, in dependency order.

    Walks forward over the same ``-- depends:`` graph the pipeline runs on, so
    the affected set is derived rather than hardcoded. Returned in the order the
    statements must be re-executed: an ordering, not a set, because recomputing
    ``rpt.desk_summary`` before ``rpt.regulatory_extract`` would rebuild it from
    stale inputs and quietly produce a second wrong number.

    ``include_break`` decides whether the broken relation itself is in the
    result, and the two callers want different answers:

    * **the embargo** wants what is *contaminated* -- the reports downstream of
      the break, which is ``include_break=False``.
    * **the recompute** wants what must be *rebuilt*, which has to start at the
      break relation itself. It is the statement that consumed the bad input, so
      it is the one that will consume the corrected input; leaving it out means
      the fix never enters the pipeline and every downstream relation
      recomputes the same wrong number from the same stale row. That is not a
      hypothetical -- it happened, and the "corrected" figure came back
      identical to the original. See docs/BUILD_LOG.md, "Surprise 25".
    """
    affected: set[str] = set()
    frontier = [break_relation]
    while frontier:
        current = frontier.pop()
        for child in source.downstream(current):
            if child not in affected:
                affected.add(child)
                frontier.append(child)

    if include_break and not source.is_source_relation(break_relation):
        affected.add(break_relation)

    ordered = [st.target for st in source.ordered() if st.target in affected]
    return tuple(ordered)


def recompute_order(source: SQLSource, break_relation: str) -> tuple[str, ...]:
    """What must be rebuilt, in the order to rebuild it -- break relation first."""
    return blast_radius(source, break_relation, include_break=True)


def affected_reports(source: SQLSource, break_relation: str) -> tuple[str, ...]:
    """The subset of the blast radius a human actually reads."""
    return tuple(
        r for r in blast_radius(source, break_relation)
        if r.startswith(f"{REPORT_SCHEMA}.")
    )


# ---------------------------------------------------------------------------
# correction and recompute
# ---------------------------------------------------------------------------


def apply_correction(con, sql: str | None) -> int:
    """Apply an operator-supplied data correction, returning rows changed.

    REMEDIATE does not invent the fix. A missing FX rate arriving late is an
    upstream event; this just applies whatever correction was handed to it, so
    the recompute has something different to compute from. Empty or None means
    "the data is already corrected", which is the normal case when the fix came
    from a vendor redelivery rather than from a person with a SQL prompt.
    """
    if not sql or not sql.strip():
        return 0
    before = con.execute("SELECT COUNT(*) FROM raw.fx_rates").fetchone()[0]
    con.execute(sql)
    after = con.execute("SELECT COUNT(*) FROM raw.fx_rates").fetchone()[0]
    return after - before


@dataclass(frozen=True)
class Recomputed:
    relation: str
    rows: int
    seconds: float


def recompute_relation(con, source: SQLSource, relation: str, *, run_id: str) -> Recomputed:
    """Re-execute the statement that produces ``relation``.

    Runs the statement's own SQL text rather than re-reading the file, so this
    works identically against a version-pinned checkout and the working tree.
    """
    import time

    statement = source.get(relation)
    if statement is None:
        raise RemediationError(
            f"nothing in this source produces {relation!r}, so it cannot be recomputed"
        )
    ensure_tables(con)
    t0 = time.perf_counter()
    con.execute(statement.sql)
    elapsed = time.perf_counter() - t0
    rows = con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]
    con.execute(
        f"INSERT INTO {RECOMPUTE_TABLE} "
        "(run_id, relation, rows_after, seconds, recomputed_at) VALUES (?, ?, ?, ?, ?)",
        [run_id, relation, rows, round(elapsed, 4), _now()],
    )
    return Recomputed(relation=relation, rows=int(rows), seconds=round(elapsed, 4))


def recomputed_in_run(con, run_id: str) -> tuple[str, ...]:
    """What this run rebuilt, in the order it happened. Read-only tolerant."""
    ensure_tables(con, strict=False)
    try:
        rows = con.execute(
            f"SELECT relation FROM {RECOMPUTE_TABLE} WHERE run_id = ? "
            "ORDER BY recomputed_at",
            [run_id],
        ).fetchall()
    except Exception:
        return ()
    return tuple(r[0] for r in rows)


def measure(con, relation: str, column: str, selector: str | None = None) -> float | None:
    """The current value of the figure under remediation."""
    where = f"WHERE {selector}" if selector else ""
    row = con.execute(f'SELECT SUM("{column}") FROM {relation} {where}').fetchone()
    return float(row[0]) if row and row[0] is not None else None


# ---------------------------------------------------------------------------
# notification -- mocked on purpose
# ---------------------------------------------------------------------------


def notify_external(
    con,
    *,
    run_id: str,
    subject: str,
    body: str,
    payload: dict | None = None,
    channel: str = "MOCK",
    recipient: str = "external-consumers@example.invalid",
) -> int:
    """Record that external consumers were told a published figure was wrong.

    **Mocked deliberately.** Telling a regulator or a client that a published
    number was wrong is the one genuinely irreversible action in this project,
    and a demo must not be one misconfiguration away from performing it. The
    row written here is the evidence that the notification *would* have gone,
    which is all the workflow needs to prove.
    """
    ensure_tables(con)
    con.execute(
        f"INSERT INTO {NOTIFY_TABLE} "
        "(run_id, sent_at, channel, recipient, subject, body, payload) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [run_id, _now(), channel, recipient, subject, body,
         json.dumps(payload or {}, default=str)],
    )
    return 1


def notifications_for(con, run_id: str) -> tuple[tuple, ...]:
    """What this run told the outside world. Read-only tolerant."""
    ensure_tables(con, strict=False)
    try:
        return tuple(con.execute(
            f"SELECT run_id, channel, recipient, subject FROM {NOTIFY_TABLE} "
            "WHERE run_id = ?", [run_id],
        ).fetchall())
    except Exception:
        return ()


# ---------------------------------------------------------------------------
# the evidence a human is shown at the gate
# ---------------------------------------------------------------------------


@dataclass
class ApprovalEvidence:
    """What the approver needs in order to answer the question honestly."""

    relation: str
    column: str
    selector: str
    break_relation: str | None = None
    break_step: str | None = None
    break_file: str | None = None
    blamed_branch: str | None = None
    original_value: float | None = None
    corrected_value: float | None = None
    code_version: str | None = None
    code_provenance: str | None = None
    recomputed: Sequence[str] = field(default_factory=tuple)
    embargoed: Sequence[str] = field(default_factory=tuple)

    @property
    def delta(self) -> float | None:
        if self.original_value is None or self.corrected_value is None:
            return None
        return self.corrected_value - self.original_value

    def render(self) -> str:
        """Markdown for the approval gate's body. Read under time pressure."""
        def money(v: float | None) -> str:
            return f"{v:,.2f}" if v is not None else "unknown"

        version = self.code_version[:12] if self.code_version else "UNPINNED"
        lines = [
            f"**Recompute complete. Notify external consumers that a published "
            f"figure was wrong?**",
            "",
            f"| | |",
            f"|---|---|",
            f"| figure | `{self.relation}.{self.column}` |",
            f"| row | `{self.selector}` |",
            f"| published (wrong) | **{money(self.original_value)}** |",
            f"| corrected | **{money(self.corrected_value)}** |",
            f"| delta | **{money(self.delta)}** |",
            "",
            "**Root cause**",
            "",
            f"- break at `{self.break_relation}` in CTE `{self.break_step}` "
            f"({self.break_file})",
            f"- responsible branch: `{self.blamed_branch}`",
            f"- code version the original ran under: `{version}` "
            f"(via {self.code_provenance or 'unknown'})",
            "",
            f"**Recomputed in dependency order:** "
            f"{', '.join(self.recomputed) if self.recomputed else 'nothing'}",
            "",
            f"**Currently embargoed:** "
            f"{', '.join(self.embargoed) if self.embargoed else 'nothing'}",
            "",
            "Approving records an external notification. Rejecting leaves the "
            "corrected figures in place and notifies no one; the embargo lifts "
            "either way.",
        ]
        return "\n".join(lines)


def correction_target(sql: str | None) -> str | None:
    """The relation an operator's correction writes to, parsed out of the SQL.

    Which relations need rebuilding depends on where the *fix* landed, not on
    where the symptom appeared. The break relation is downstream of the
    correction -- for BUG A the fix inserts into ``raw.fx_rates`` while the
    break is at ``trd.trades_converted``, three relations later -- so a
    recompute starting at the break rebuilds everything except the two
    relations that actually needed it, and the corrected figure comes back
    identical to the wrong one. Twice, in this project's case, one level apart.
    See docs/BUILD_LOG.md, "Surprise 25".

    Parsed with sqlglot rather than taken as a parameter so it cannot be set
    wrong, and so a correction written by hand at 2am still rebuilds the right
    things. Returns None when the target cannot be determined, and the caller
    falls back to the break relation.
    """
    if not sql or not sql.strip():
        return None
    try:
        import sqlglot
        from sqlglot import exp

        tree = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return None

    for node_type in (exp.Insert, exp.Update, exp.Delete):
        node = tree if isinstance(tree, node_type) else tree.find(node_type)
        if node is None:
            continue
        table = node.find(exp.Table)
        if table is not None:
            return table.sql(dialect="duckdb").split(" ")[0]
    return None


def rebuild_from(source: SQLSource, correction_sql: str | None, break_relation: str) -> str:
    """Where a recompute must start: the corrected relation if known, else the break."""
    target = correction_target(correction_sql)
    if target and (target in source.by_target or source.downstream(target)):
        return target
    return break_relation
