"""SQLSource over a dbt project.

Reads ``target/manifest.json`` for the node graph and ``target/compiled/``
for each model's SQL after ``ref()`` and ``source()`` resolution. Compiled
SQL is what the warehouse actually executed, so it is what the parser must
consume -- Jinja-templated source would require branchlin to reimplement
dbt's renderer.

This is an optional adapter. Its dependencies live in
``requirements-dbt.txt`` and are not installed in the core image.

Not implemented. Phase 1.
"""
