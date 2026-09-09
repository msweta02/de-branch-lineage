"""SQLSource over a dbt project.

Reads ``target/manifest.json`` for the node graph and ``target/compiled/``
for each model's SQL after ``ref()`` and ``source()`` resolution. Compiled
SQL is what the warehouse actually executed, so it is what the parser must
consume -- Jinja-templated source would require branchlin to reimplement
dbt's renderer.

This is an optional adapter. Its dependencies live in
``requirements-dbt.txt`` and are not installed in the core image.

**Still a stub after Phase 1, by decision rather than omission.** The dbt
fixture under ``include/dbt_fixture/`` already demonstrates the gap this
project exists to fill -- BUG A reproduces there and dbt's column-level lineage
does not point at the join -- so implementing the adapter proves nothing new
about the argument. It matters for *adoption*, not for the demonstration, and
Phase 2 has the approval gate to build. The interface it must satisfy is
:class:`branchlin.sources.SQLSource`, which
:mod:`branchlin.sources.rawfiles` now implements and exercises.
"""
