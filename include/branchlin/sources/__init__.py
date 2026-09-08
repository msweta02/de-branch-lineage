"""Where SQL comes from.

Defines the abstract ``SQLSource`` interface: given a pipeline, yield its
statements in dependency order, each tagged with the identity of the relation
it produces and the relations it consumes.

The split exists so that :mod:`branchlin.parser` never learns whether it is
looking at a hand-written ``.sql`` file or at dbt's compiled output. Two
implementations ship:

* :mod:`branchlin.sources.rawfiles` -- ordered ``.sql`` files on disk.
* :mod:`branchlin.sources.dbt`      -- a dbt ``manifest.json`` plus
  ``target/compiled/``.

Adding a third (SQLMesh, a warehouse's query history) should require touching
nothing outside this package.

Not implemented. Phase 1.
"""
