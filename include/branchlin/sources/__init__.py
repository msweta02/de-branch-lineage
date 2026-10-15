"""Where SQL comes from.

Defines the abstract :class:`SQLSource` interface: given a pipeline, yield its
statements in dependency order, each tagged with the identity of the relation
it produces and the relations it consumes.

The split exists so that :mod:`branchlin.parser` never learns whether it is
looking at a hand-written ``.sql`` file or at dbt's compiled output. Two
implementations are specified:

* :mod:`branchlin.sources.rawfiles` -- ordered ``.sql`` files on disk.
* :mod:`branchlin.sources.dbt`      -- a dbt ``manifest.json`` plus
  ``target/compiled/`` (still a stub; the dbt fixture already demonstrates the
  gap, so the adapter is not a Phase 1 priority).

Adding a third (SQLMesh, a warehouse's query history) should require touching
nothing outside this package.

**Ordering.** Dependency order comes from the ``-- depends:`` header in each
statement, and from nothing else. That is the same declaration
``include/runner.py`` executes on, so the tracer walks the graph the pipeline
actually ran. Filename prefixes are a human convenience and are not consulted
except to break ties between statements that are genuinely independent.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import Iterable, Iterator

from branchlin import parser
from branchlin.parser import ParsedStatement


class CycleError(ValueError):
    """The ``-- depends:`` headers describe a cycle."""


@dataclass(frozen=True)
class Statement:
    """One SQL statement, with its declared output and inputs."""

    target: str
    depends: tuple[str, ...]
    sql: str
    #: Where this came from, for display: a repo-relative path, or a dbt node id.
    origin: str

    @property
    def module(self) -> str:
        """``02_trades/05_convert_currency.sql`` -> ``02_trades``."""
        return self.origin.split("/")[0] if "/" in self.origin else ""


class SQLSource(ABC):
    """A pipeline's SQL, addressable by the relation each statement produces."""

    @abstractmethod
    def statements(self) -> Iterable[Statement]:
        """Yield every statement. Order need not be meaningful."""

    # -- lookups -----------------------------------------------------------

    @cached_property
    def by_target(self) -> dict[str, Statement]:
        out: dict[str, Statement] = {}
        for st in self.statements():
            if st.target is None:
                continue
            out[st.target] = st
        return out

    @property
    def targets(self) -> tuple[str, ...]:
        return tuple(self.by_target)

    def get(self, relation: str) -> Statement | None:
        return self.by_target.get(relation)

    def parse(self, relation: str) -> ParsedStatement:
        """Parse the statement producing ``relation``, memoised per source."""
        if not hasattr(self, "_parse_cache"):
            self._parse_cache: dict[str, ParsedStatement] = {}
        if relation not in self._parse_cache:
            st = self.by_target.get(relation)
            if st is None:
                raise KeyError(f"no statement produces {relation!r}")
            self._parse_cache[relation] = parser.parse(st.sql)
        return self._parse_cache[relation]

    def is_source_relation(self, relation: str) -> bool:
        """True when nothing in this source produces ``relation`` -- a raw input."""
        return relation not in self.by_target

    # -- graph -------------------------------------------------------------

    def upstream(self, relation: str) -> tuple[str, ...]:
        """Relations that ``relation`` is declared to depend on."""
        st = self.by_target.get(relation)
        return st.depends if st is not None else ()

    def downstream(self, relation: str) -> tuple[str, ...]:
        """Relations that declare a dependency on ``relation``."""
        return tuple(
            st.target for st in self.by_target.values() if relation in st.depends
        )

    def ordered(self) -> tuple[Statement, ...]:
        """Every statement in dependency order.

        Kahn's algorithm over the declared headers. Ties -- statements whose
        dependencies are all satisfied at the same time -- are broken on
        ``origin`` so the order is stable and matches the runner's file order.
        """
        remaining = dict(self.by_target)
        done: set[str] = set()
        out: list[Statement] = []
        while remaining:
            ready = [
                st for st in remaining.values()
                if all(d in done or d not in remaining for d in st.depends)
            ]
            if not ready:
                raise CycleError(
                    "cycle among: " + ", ".join(sorted(remaining))
                )
            ready.sort(key=lambda st: st.origin)
            for st in ready:
                out.append(st)
                done.add(st.target)
                del remaining[st.target]
        return tuple(out)

    def paths(self, start: str, end: str) -> tuple[tuple[str, ...], ...]:
        """Every downstream path from ``start`` to ``end``, following headers."""
        found: list[tuple[str, ...]] = []

        def walk(node: str, acc: tuple[str, ...]) -> None:
            if node in acc:  # defensive: headers should be acyclic
                return
            acc = acc + (node,)
            if node == end:
                found.append(acc)
                return
            for nxt in sorted(self.downstream(node)):
                walk(nxt, acc)

        walk(start, ())
        return tuple(found)

    def longest_path(self, start: str, end: str) -> tuple[str, ...]:
        """The longest path from ``start`` to ``end``.

        The longest path is the one the demo quotes: it is the full distance a
        fault can travel before it surfaces. Shorter paths between the same two
        relations are real but understate the hop count.
        """
        paths = self.paths(start, end)
        if not paths:
            raise KeyError(f"no path from {start!r} to {end!r}")
        return max(paths, key=len)

    def reverse_path(self, start: str, end: str) -> tuple[str, ...]:
        """:meth:`longest_path` in the direction the tracer walks it."""
        return tuple(reversed(self.longest_path(start, end)))

    # -- aggregate branch-point counting -----------------------------------

    def count_branch_points(self, relations: Iterable[str]) -> dict[str, int]:
        """Sum branch points over ``relations``.

        This is the counter behind the figures in docs/technical-reference.md,
        "Branch points on the BUG A path". It exists as library code, with a
        test over it, precisely so those figures stop being the output of a
        throwaway script.
        """
        total: dict[str, int] = {}
        for rel in relations:
            if self.is_source_relation(rel):
                continue
            for k, v in self.parse(rel).counts().items():
                total[k] = total.get(k, 0) + v
        total.setdefault("documented", 0)
        return total


__all__ = ["SQLSource", "Statement", "CycleError"]
