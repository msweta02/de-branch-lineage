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

Not implemented. Phase 1.
"""
