"""Point queries against the warehouse, one hop at a time.

Given a branch point identified by :mod:`branchlin.parser` and a key
identifying the row under investigation, the prober issues the narrowest
query that answers "what did this row look like entering this hop, and what
did it look like leaving it".

Connections are resolved through Airflow's connection registry rather than
hardcoded, so the same trace runs against DuckDB locally and against a real
warehouse in a deployment.

Probes must be read-only and cheap: a trace crosses ten-plus hops, and a
diagnosis that costs a full table scan per hop is a diagnosis nobody runs.
Every probe is a single aggregate over a predicate -- ``COUNT(*)``,
``COUNT(col)`` and ``SUM(col)`` in one pass. There is no ``SELECT *`` in this
module, by design: pulling rows back to compare them in Python would make the
cost of a trace proportional to the size of the data rather than to the number
of hops.

**On the ``duckdb_writer`` pool.** Phase 0 serialises writes through a one-slot
pool because DuckDB permits a single writer. The natural assumption is that
read-only probes are exempt. They are not: DuckDB's lock is on the *file*, and
a ``read_only=True`` connection opened while a writer holds it fails with
``Could not set lock on file ... Conflicting lock is held``. Measured, not
assumed -- see docs/BUILD_LOG.md, "Surprise 16". A DIAGNOSE task must therefore
take the same ``duckdb_writer`` slot as a pipeline task, and
:data:`REQUIRES_WRITER_POOL` records that so the Phase 2 DAG cannot forget it.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

#: Airflow connection that names the warehouse. Declared in airflow_settings.yaml.
DEFAULT_CONN_ID = "duckdb_warehouse"

#: Environment override, used by tests and by host-side runs.
ENV_VAR = "BRANCHLIN_WAREHOUSE"

#: Repo-relative fallback, used when neither Airflow nor the environment answers.
REPO_WAREHOUSE = Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb"

#: A read-only probe still contends with DuckDB's file lock, so a task that
#: probes must hold the writer pool slot. See the module docstring.
REQUIRES_WRITER_POOL = True
WRITER_POOL = "duckdb_writer"

#: Types SUM() is meaningful over. Anything else is counted but not summed.
_NUMERIC = re.compile(
    r"^(TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UTINYINT|USMALLINT|UINTEGER|"
    r"UBIGINT|FLOAT|DOUBLE|REAL|DECIMAL|NUMERIC)",
    re.IGNORECASE,
)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


class ProbeError(RuntimeError):
    """A probe could not be answered."""


class WarehouseLocked(ProbeError):
    """The warehouse is held by a writer, so even a read-only probe is refused."""


def _check_identifier(name: str, what: str) -> str:
    """Reject anything that is not a plain (optionally dotted) identifier.

    The selector is a SQL predicate by contract -- it is the caller's job to
    supply something trusted -- but relation and column names are used to build
    the query shape, and those are always identifiers. Validating them keeps a
    typo from becoming a syntax error deep inside a trace.
    """
    if not name or not _IDENT_RE.match(name):
        raise ProbeError(f"not a valid {what}: {name!r}")
    return name


@dataclass(frozen=True)
class WarehouseTarget:
    """Where the warehouse is, and how we found out."""

    path: Path
    #: "argument" | "environment" | "airflow:<conn_id>" | "repo-default"
    provenance: str

    def __str__(self) -> str:
        return f"{self.path} (via {self.provenance})"


def _from_airflow(conn_id: str) -> Path | None:
    """Resolve the warehouse path from Airflow's connection registry.

    Importable both inside a worker (Task SDK) and from a plain process with
    the metadata DB configured. Returns None when Airflow is not available or
    the connection is not defined -- callers fall back rather than fail, so a
    trace can be run from a host shell during development.
    """
    try:  # Task SDK first: this is the path inside a running task.
        from airflow.sdk import BaseHook  # type: ignore
    except Exception:
        try:
            from airflow.hooks.base import BaseHook  # type: ignore
        except Exception:
            return None
    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception:
        return None
    extra = {}
    try:
        extra = conn.extra_dejson or {}
    except Exception:
        pass
    candidate = extra.get("database") or extra.get("path") or conn.host
    if not candidate:
        return None
    return Path(str(candidate))


def resolve_warehouse(
    path: Path | str | None = None,
    *,
    conn_id: str = DEFAULT_CONN_ID,
) -> WarehouseTarget:
    """Find the warehouse, preferring the most explicit answer available.

    Order: explicit argument, then :data:`ENV_VAR`, then Airflow's connection
    registry, then the repo default. Airflow's answer is skipped when the path
    it names does not exist on this machine -- the declared connection points
    at the container path ``/usr/local/airflow/include/data/warehouse.duckdb``,
    which is correct inside a worker and meaningless on the host.
    """
    if path is not None:
        return WarehouseTarget(Path(path), "argument")

    env = os.environ.get(ENV_VAR)
    if env:
        return WarehouseTarget(Path(env), "environment")

    from_airflow = _from_airflow(conn_id)
    if from_airflow is not None and from_airflow.exists():
        return WarehouseTarget(from_airflow, f"airflow:{conn_id}")

    return WarehouseTarget(REPO_WAREHOUSE, "repo-default")


@dataclass(frozen=True)
class Probe:
    """The answer to one question about one relation.

    ``value`` is the SUM of ``column`` over the rows matching ``selector``, or
    None when the column is not numeric or was not asked for. ``rows`` is how
    many rows matched. ``nulls`` is how many of those had a NULL in ``column``
    -- the signal that actually moves under BUG A, since the row counts do not.
    """

    relation: str
    selector: str
    column: str | None
    rows: int
    nulls: int | None
    value: float | None
    column_type: str | None
    sql: str = field(compare=False, default="")

    @property
    def all_null(self) -> bool:
        return self.column is not None and self.rows > 0 and self.nulls == self.rows

    @property
    def has_nulls(self) -> bool:
        return bool(self.nulls)

    @property
    def empty(self) -> bool:
        return self.rows == 0

    def describe(self) -> str:
        bits = [f"{self.relation}", f"rows={self.rows:,}"]
        if self.column:
            bits.append(f"{self.column}={_fmt(self.value)}")
            bits.append(f"nulls={self.nulls:,}")
        return "  ".join(bits)


def _fmt(v: float | None) -> str:
    if v is None:
        return "-"
    return f"{v:,.2f}" if isinstance(v, float) else f"{v:,}"


class Prober:
    """Read-only point queries against one warehouse.

    The connection is opened lazily and reused for the life of the prober, so a
    ten-hop trace pays the open cost once.
    """

    def __init__(
        self,
        warehouse: Path | str | None = None,
        *,
        conn_id: str = DEFAULT_CONN_ID,
    ):
        self.target = resolve_warehouse(warehouse, conn_id=conn_id)
        self._con: duckdb.DuckDBPyConnection | None = None
        #: Every query issued, in order. The audit trail for a trace.
        self.issued: list[str] = []

    # -- connection --------------------------------------------------------

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            if not self.target.path.exists():
                raise ProbeError(f"no warehouse at {self.target}")
            try:
                self._con = duckdb.connect(str(self.target.path), read_only=True)
            except duckdb.IOException as exc:
                if "lock" in str(exc).lower():
                    raise WarehouseLocked(
                        f"{self.target.path} is held by a writer, so a read-only "
                        f"probe cannot open it. DuckDB locks the file, not the "
                        f"rows: a diagnosis task must hold the {WRITER_POOL!r} "
                        f"pool slot like any pipeline task. Original: {exc}"
                    ) from exc
                raise
        return self._con

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def __enter__(self) -> "Prober":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- introspection -----------------------------------------------------

    def exists(self, relation: str) -> bool:
        schema, _, table = _check_identifier(relation, "relation").partition(".")
        rows = self.connection.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchall()
        return bool(rows)

    def columns(self, relation: str) -> tuple[str, ...]:
        schema, _, table = _check_identifier(relation, "relation").partition(".")
        rows = self.connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()
        return tuple(r[0] for r in rows)

    def column_type(self, relation: str, column: str) -> str | None:
        schema, _, table = _check_identifier(relation, "relation").partition(".")
        _check_identifier(column, "column")
        row = self.connection.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND column_name = ?",
            [schema, table, column],
        ).fetchone()
        return row[0] if row else None

    # -- the probe itself --------------------------------------------------

    def probe(
        self,
        relation: str,
        column: str | None = None,
        selector: str | None = None,
    ) -> Probe:
        """One aggregate pass over the rows matching ``selector``.

        Returns the summed value, the matching row count and the null count in
        a single query. No ``SELECT *``, and no rows are transported.
        """
        _check_identifier(relation, "relation")
        if not self.exists(relation):
            raise ProbeError(f"relation does not exist: {relation}")

        where = f"WHERE {selector}" if selector else ""
        ctype = None
        parts = ["COUNT(*) AS rows"]

        if column is not None:
            _check_identifier(column, "column")
            available = self.columns(relation)
            if column not in available:
                raise ProbeError(
                    f"{relation} has no column {column!r} "
                    f"(has {', '.join(available[:8])}{'...' if len(available) > 8 else ''})"
                )
            ctype = self.column_type(relation, column)
            parts.append(f'COUNT("{column}") AS non_null')
            if ctype and _NUMERIC.match(ctype):
                parts.append(f'SUM("{column}") AS total')

        sql = f"SELECT {', '.join(parts)} FROM {relation} {where}".strip()
        self.issued.append(sql)
        try:
            row = self.connection.execute(sql).fetchone()
        except duckdb.Error as exc:
            raise ProbeError(f"probe failed on {relation}: {exc}\n  {sql}") from exc

        rows = int(row[0])
        nulls = None
        value = None
        if column is not None:
            non_null = int(row[1])
            nulls = rows - non_null
            if len(row) > 2:
                value = float(row[2]) if row[2] is not None else None

        return Probe(
            relation=relation,
            selector=selector or "",
            column=column,
            rows=rows,
            nulls=nulls,
            value=value,
            column_type=ctype,
            sql=sql,
        )

    def distinct_values(
        self,
        relation: str,
        column: str,
        selector: str | None = None,
        limit: int = 20,
    ) -> tuple[tuple, ...]:
        """Distinct values of one column, with counts. Used to report branches taken.

        Bounded by ``limit`` so a high-cardinality column cannot turn a trace
        into a table dump.
        """
        _check_identifier(relation, "relation")
        _check_identifier(column, "column")
        where = f"WHERE {selector}" if selector else ""
        sql = (
            f'SELECT "{column}" AS v, COUNT(*) AS n FROM {relation} {where} '
            f'GROUP BY 1 ORDER BY n DESC LIMIT {int(limit)}'
        ).strip()
        self.issued.append(sql)
        try:
            return tuple(self.connection.execute(sql).fetchall())
        except duckdb.Error as exc:
            raise ProbeError(f"distinct_values failed on {relation}: {exc}") from exc
