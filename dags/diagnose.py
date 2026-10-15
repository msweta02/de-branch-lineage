"""DIAGNOSE -- trace a wrong figure back to the branch that broke it.

Takes the four tracer inputs as params and runs an INVESTIGATE trace, halting at
the first hop that does not account for its inputs. The verdict goes to XCom for
downstream tasks and to the task log, formatted, for a human.

There is no plugin. The task log is the report and the Required Actions tab is
where the approval in REMEDIATE appears -- building a UI would have cost more
than it demonstrated.

(This module mentions airflow and dag explicitly so Airflow's safe-mode DAG
discovery will open it: a file that imports its DAG from a helper is invisible
unless both strings appear in the source. See docs/build-log.md, Surprise 7.)
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, task
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

REMEDIATE_DAG_ID = "remediate"

DOC = __doc__


@dag(
    dag_id="diagnose",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["de-branch-lineage", "diagnose"],
    doc_md=DOC,
    default_args={"retries": 0},
    params={
        "relation": Param(
            "rpt.desk_summary", type="string",
            description="The relation holding the wrong figure.",
        ),
        "selector": Param(
            "business_date = DATE '2026-09-03'", type="string",
            description=(
                "A predicate identifying the row, not an id. Report rows are "
                "aggregates and the grouping columns are what identify them."
            ),
        ),
        "column": Param(
            "total_pnl_usd", type="string",
            description="The measure that is wrong.",
        ),
        "as_of": Param(
            "2026-09-03", type="string",
            description=(
                "The date under investigation. Selects which run's recorded "
                "code version the SQL is read at."
            ),
        ),
        "trigger_remediate": Param(
            False, type="boolean",
            description="Fire the REMEDIATE DAG automatically if a break is found.",
        ),
        "correction_sql": Param(
            "", type="string",
            description="Passed through to REMEDIATE as the data fix to apply.",
        ),
    },
)
def diagnose():
    @task(
        # A read-only probe is NOT exempt from DuckDB's file lock: the lock is
        # on the file, so a read-only connection opened while a pipeline task
        # holds it is refused outright. Measured in Phase 1 -- see
        # docs/build-log.md, "Surprise 16", and prober.REQUIRES_WRITER_POOL.
        pool="duckdb_writer",
    )
    def investigate(**context) -> dict:
        """Walk backwards from the figure and stop at the first break."""
        from branchlin import pinning
        from branchlin.prober import Prober
        from branchlin.tracer import Tracer

        params = context["params"]
        relation = params["relation"]
        selector = params["selector"]
        column = params["column"]
        as_of = params["as_of"]

        steps: list[str] = []
        version, provenance = pinning.version_for_run(
            "04_reporting_pipeline", str(as_of), trace=steps
        )
        pinned = pinning.source_for(as_of)

        print("=" * 100)
        print(f"DIAGNOSE  {relation}.{column}  WHERE {selector}")
        print(f"as of {as_of}")
        print(f"code version: {pinned.describe()}")
        for step in steps:
            print(f"  resolution: {step}")
        print("=" * 100)

        with Prober() as prober:
            tracer = Tracer(pinned.source, prober, source_version=pinned.version)
            trace = tracer.investigate(relation, selector, column, as_of=as_of)
            print(trace.render())

        summary: dict = {
            "verdict": "BREAK" if trace.broke else "NO_BREAK",
            "relation": relation,
            "column": column,
            "selector": selector,
            "as_of": str(as_of),
            "code_version": pinned.version,
            "code_provenance": pinned.provenance,
            "relations_walked": list(trace.relations),
            "hops": trace.hop_count,
            "original_value": trace.hops[0].probe.value if trace.hops and trace.hops[0].probe else None,
        }

        if trace.broke:
            hop = trace.break_hop
            blame = hop.verdict.blame[0] if hop.verdict.blame else None
            summary.update({
                "break_relation": hop.relation,
                "break_step": hop.step,
                "break_file": hop.origin,
                "break_reason": hop.verdict.reason,
                "blamed_branch": blame.describe() if blame else None,
                "blamed_kind": blame.kind if blame else None,
                "untrusted": list(trace.untrusted),
            })
            print()
            print(f"VERDICT: BREAK at {hop.relation} in CTE {hop.step!r} ({hop.origin})")
            if blame:
                print(f"         responsible: {blame.describe()}")
            print(f"         {len(trace.untrusted)} upstream relation(s) untrusted")
        else:
            print()
            print(f"VERDICT: NO BREAK -- complete path to source over "
                  f"{trace.hop_count} hops")

        return summary

    verdict = investigate()

    # Only continue when there is something to remediate, and only when the
    # caller asked for it. A diagnosis is read far more often than it is acted
    # on, so automatic remediation is opt-in rather than the default.
    def _should_remediate(summary: dict, **context) -> bool:
        if not context["params"].get("trigger_remediate"):
            print("trigger_remediate is false -- stopping after the diagnosis")
            return False
        broke = summary.get("verdict") == "BREAK"
        print(f"verdict={summary.get('verdict')} -> "
              f"{'triggering REMEDIATE' if broke else 'nothing to remediate'}")
        return broke

    gate = ShortCircuitOperator(
        task_id="break_found",
        python_callable=_should_remediate,
        op_args=[verdict],
    )

    fire = TriggerDagRunOperator(
        task_id="trigger_remediate",
        trigger_dag_id=REMEDIATE_DAG_ID,
        wait_for_completion=False,
        reset_dag_run=True,
        conf={
            "break_relation": "{{ ti.xcom_pull(task_ids='investigate')['break_relation'] }}",
            "break_step": "{{ ti.xcom_pull(task_ids='investigate')['break_step'] }}",
            "break_file": "{{ ti.xcom_pull(task_ids='investigate')['break_file'] }}",
            "blamed_branch": "{{ ti.xcom_pull(task_ids='investigate')['blamed_branch'] }}",
            "relation": "{{ params.relation }}",
            "column": "{{ params.column }}",
            "selector": "{{ params.selector }}",
            "as_of": "{{ params.as_of }}",
            "code_version": "{{ ti.xcom_pull(task_ids='investigate')['code_version'] }}",
            "code_provenance": "{{ ti.xcom_pull(task_ids='investigate')['code_provenance'] }}",
            "original_value": "{{ ti.xcom_pull(task_ids='investigate')['original_value'] }}",
            "correction_sql": "{{ params.correction_sql }}",
        },
    )

    verdict >> gate >> fire


diagnose()
