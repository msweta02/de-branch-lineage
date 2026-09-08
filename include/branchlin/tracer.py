"""Orchestrates the backward walk.

Entry point for DIAGNOSE. Given a wrong value in a report -- a table, a
measure, and a set of key columns -- the tracer walks upstream in reverse DAG
order, asking :mod:`branchlin.prober` for evidence and
:mod:`branchlin.reconciler` for a verdict at each hop, and halts at the first
imbalance.

The SQL it parses is read from the DAG bundle version that *actually ran* on
the date in question, not from today's working tree. A pipeline that was
fixed last week must still be diagnosable as it behaved last month. That
requirement is the argument for living inside the orchestrator: only the
orchestrator knows which code produced which run.

Not implemented. Phase 1.
"""
