"""Point queries against the warehouse, one hop at a time.

Given a branch point identified by :mod:`branchlin.parser` and a key
identifying the row under investigation, the prober issues the narrowest
query that answers "what did this row look like entering this hop, and what
did it look like leaving it".

Connections are resolved through Airflow's connection registry rather than
hardcoded, so the same trace runs against DuckDB locally and against a real
warehouse in a deployment.

Probes must be read-only and cheap: a trace crosses fifteen-plus hops, and a
diagnosis that costs a full table scan per hop is a diagnosis nobody runs.

Not implemented. Phase 1.
"""
