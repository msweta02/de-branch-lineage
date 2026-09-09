"""branchlin -- branch-level lineage for Airflow.

Column-level lineage tells you that ``net_amount`` derives from ``amt_net``.
It does not tell you *which branch* a row took: which CASE arm evaluated,
which WHERE predicate excluded it, which LEFT JOIN missed. dbt's own
column-level lineage documentation states it reflects lineage from SELECT
statements and "doesn't reflect other usage like joins and filters".

branchlin fills that gap. It walks a pipeline backwards in dependency order,
reconciling the arithmetic at each hop and reporting the branch each row
took, stopping at the first hop that fails to balance.

Phase 1 -- the DIAGNOSE engine -- is implemented:

* :mod:`branchlin.parser`    -- SQL to branch points, via sqlglot.
* :mod:`branchlin.sources`   -- where SQL comes from; ``rawfiles`` implemented.
* :mod:`branchlin.pushdown`  -- rewriting a selector backwards through a hop.
* :mod:`branchlin.prober`    -- narrow read-only point queries.
* :mod:`branchlin.reconciler`-- does this hop account for its inputs?
* :mod:`branchlin.tracer`    -- the walk, in INVESTIGATE and EXPLAIN modes.
* :mod:`branchlin.pinning`   -- which code ran on the date under investigation.

Phase 2 -- REMEDIATE, the human approval gate and any UI -- is not built.
:mod:`branchlin.sources.dbt` remains a stub by decision, not omission.

Typical use::

    from branchlin.prober import Prober
    from branchlin.sources.rawfiles import RawFilesSource
    from branchlin.tracer import Tracer

    tracer = Tracer(RawFilesSource(), Prober())
    trace = tracer.investigate(
        "rpt.desk_summary",
        "business_date = DATE '2026-09-03'",
        "total_pnl_usd",
        as_of="2026-09-03",
    )
    print(trace.render())
"""

__version__ = "0.1.0"
