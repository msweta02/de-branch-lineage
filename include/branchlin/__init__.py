"""branchlin -- branch-level lineage for Airflow.

Column-level lineage tells you that ``net_amount`` derives from ``amt_net``.
It does not tell you *which branch* a row took: which CASE arm evaluated,
which WHERE predicate excluded it, which LEFT JOIN missed. dbt's own
column-level lineage documentation states it reflects lineage from SELECT
statements and "doesn't reflect other usage like joins and filters".

branchlin fills that gap. It walks a pipeline backwards in DAG order,
reconciling the arithmetic at each hop and reporting the branch each row
took, stopping at the first hop that fails to balance.

Nothing in this package is implemented yet -- Phase 0 reserves the shape.
"""

__version__ = "0.0.0"
