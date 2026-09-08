"""SQLSource over plain ``.sql`` files.

Reads the numbered modules under ``include/sql/`` -- ``01_reference``,
``02_trades``, ``03_positions``, ``04_reporting`` -- and yields their
statements in DAG order. Ordering comes from the Airflow task and asset graph
that executes them, not from the filename prefixes; the prefixes are a human
convenience and are not trusted as a dependency declaration.

Not implemented. Phase 1.
"""
