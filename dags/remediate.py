"""REMEDIATE -- put a wrong published figure right, and ask before telling anyone.

Triggered when DIAGNOSE returns a BREAK. The shape is the point:

    SETUP     embargo        mark the affected reports not-for-publication
    TASK      blast_radius   walk forward from the break over the same graph
    TASK      correction     apply the operator-supplied data fix
    TASK      recompute      re-run the affected relations, dependency order
    TASK      measure        read the corrected figure
    GATE      approval       a human decides whether to notify anyone outside
    TASK      notify         MOCKED -- writes a row, sends nothing
    TEARDOWN  lift_embargo   runs on SUCCESS **OR FAILURE**

The teardown is the most important task here. A remediation that dies halfway
through must not leave the warehouse permanently unpublishable, and it must not
require a human to remember to clean up. Airflow's setup/teardown semantics give
that for free; a script would have to get its own `finally` right, and would
still lose if the process were killed.

The approval gate is an ApprovalOperator and is not bypassable: `notify` is
downstream of it, so on rejection the notification is skipped. Notifying an
external consumer that a published number was wrong is the one genuinely
irreversible action in this project, which is why it is mocked *and* gated.

(Mentions airflow and dag explicitly for safe-mode discovery -- see
docs/build-log.md, Surprise 7.)
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, task
from airflow.providers.standard.operators.hitl import ApprovalOperator

DOC = __doc__

#: The BUG A correction, offered as the default so the demo is one click.
#: The rate is the value the generator would have produced for that date; a real
#: remediation would take it from the vendor's redelivery.
DEFAULT_CORRECTION = (
    "INSERT INTO raw.fx_rates "
    "(rate_date, ccy, quote_ccy, rate, rate_source, ingested_at) "
    "SELECT DATE '2026-09-03', 'GBP', 'USD', 1.270854, 'LATE_REDELIVERY', now() "
    "WHERE NOT EXISTS ("
    "  SELECT 1 FROM raw.fx_rates "
    "  WHERE rate_date = DATE '2026-09-03' AND ccy = 'GBP'"
    ")"
)


@dag(
    dag_id="remediate",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["de-branch-lineage", "remediate"],
    doc_md=DOC,
    default_args={"retries": 0},
    params={
        "break_relation": Param("trd.trades_converted", type="string"),
        "break_step": Param("converted", type=["string", "null"]),
        "break_file": Param("02_trades/05_convert_currency.sql", type=["string", "null"]),
        "blamed_branch": Param("", type=["string", "null"]),
        "relation": Param("rpt.desk_summary", type="string"),
        "column": Param("total_pnl_usd", type="string"),
        "selector": Param("business_date = DATE '2026-09-03'", type="string"),
        "as_of": Param("2026-09-03", type="string"),
        "code_version": Param("", type=["string", "null"]),
        "code_provenance": Param("", type=["string", "null"]),
        "original_value": Param("", type=["string", "number", "null"]),
        "correction_sql": Param(
            DEFAULT_CORRECTION, type="string",
            description="Data fix to apply before recomputing. Empty = already fixed.",
        ),
        "fail_recompute": Param(
            False, type="boolean",
            description=(
                "Test hook. Forces the recompute to fail so the teardown's "
                "embargo lift can be proved on the failure path."
            ),
        ),
    },
)
def remediate():
    # ---- SETUP: embargo -------------------------------------------------
    @task(pool="duckdb_writer")
    def embargo(**context) -> list[str]:
        """Mark every affected report as not-for-publication."""
        import duckdb
        from branchlin import remediate as R
        from branchlin.prober import resolve_warehouse
        from branchlin.sources.rawfiles import RawFilesSource

        params = context["params"]
        run_id = context["run_id"]
        source = RawFilesSource()
        reports = R.affected_reports(source, params["break_relation"])

        con = duckdb.connect(str(resolve_warehouse().path))
        try:
            held = R.set_embargo(
                con, reports, run_id=run_id,
                reason=(
                    f"remediation of {params['relation']}.{params['column']} "
                    f"after break at {params['break_relation']}"
                ),
            )
            print(f"EMBARGO SET on {len(held)} relation(s): {', '.join(held)}")
            print(f"currently embargoed: {', '.join(R.embargoed_relations(con))}")
        finally:
            con.close()
        return list(held)

    # ---- blast radius ---------------------------------------------------
    # No pool: this reads the -- depends: headers off disk and touches no
    # warehouse, so it cannot contend for DuckDB's file lock.
    @task
    def blast_radius(**context) -> list[str]:
        """Everything downstream of the break, in the order it must be rebuilt."""
        from branchlin import remediate as R
        from branchlin.sources.rawfiles import RawFilesSource

        params = context["params"]
        source = RawFilesSource()
        # Rebuild from wherever the *correction* landed, not from the break.
        # The break is downstream of the fix -- for BUG A the fix inserts into
        # raw.fx_rates while the break is at trd.trades_converted -- so
        # starting at the break skips exactly the relations that needed
        # rebuilding and "corrects" the figure to the same wrong number.
        start = R.rebuild_from(
            source, params.get("correction_sql"), params["break_relation"]
        )
        affected = R.recompute_order(source, start)
        reports = R.affected_reports(source, params["break_relation"])
        print(f"correction lands on: {start}")
        print(f"BLAST RADIUS from {start}: "
              f"{len(affected)} relation(s) to rebuild, "
              f"{len(reports)} of them reports (embargoed)")
        for i, relation in enumerate(affected, 1):
            mark = "  <- report" if relation in reports else ""
            print(f"  {i:>2}. {relation}{mark}")
        return list(affected)

    # ---- the affected reports, as the mapped input ----------------------
    @task
    def affected_report_list(**context) -> list[str]:
        """Just the reports a human reads -- the set the embargo covers."""
        from branchlin import remediate as R
        from branchlin.sources.rawfiles import RawFilesSource

        reports = R.affected_reports(
            RawFilesSource(), context["params"]["break_relation"]
        )
        print(f"affected reports: {', '.join(reports)}")
        return list(reports)

    # ---- correction -----------------------------------------------------
    @task(pool="duckdb_writer")
    def correction(**context) -> int:
        """Apply the operator-supplied data fix, if any."""
        import duckdb
        from branchlin import remediate as R
        from branchlin.prober import resolve_warehouse

        sql = context["params"].get("correction_sql") or ""
        con = duckdb.connect(str(resolve_warehouse().path))
        try:
            changed = R.apply_correction(con, sql)
        finally:
            con.close()
        if not sql.strip():
            print("no correction supplied -- assuming the data is already fixed")
        else:
            print(f"CORRECTION applied, raw.fx_rates changed by {changed:+d} row(s)")
        return changed

    # ---- recompute ------------------------------------------------------
    @task(pool="duckdb_writer")
    def recompute(affected: list[str], **context) -> list[dict]:
        """Re-execute every affected relation, in dependency order.

        **This is a fold, not a map, and that is the whole point.** It was first
        written as a dynamically-mapped task, one mapped instance per relation,
        which is the obvious shape and reads well in the UI. It produced the
        wrong answer. Mapped instances are not scheduled in map-index order --
        the first run started at index 7 -- and even pinned to a single slot by
        `max_active_tis_per_dag=1` plus the `duckdb_writer` pool, the observed
        order was:

            positions_adjusted, positions_netted, desk_summary,
            positions_accrued, book_positions, regulatory_extract,
            trades_classified, positions_marked, recon_control, pnl_daily

        `rpt.desk_summary` was rebuilt third, from inputs that had not been
        rebuilt yet, and the "corrected" figure came back as 501,622.18 -- the
        original wrong number. Serialising execution is not the same as ordering
        it, and a recompute whose steps depend on each other cannot be expressed
        as a map. See docs/build-log.md, "Surprise 24".

        Per-relation visibility is kept by the mapped `verify_report` tasks
        below, which genuinely are independent of one another.
        """
        import duckdb
        from branchlin import remediate as R
        from branchlin.prober import resolve_warehouse
        from branchlin.sources.rawfiles import RawFilesSource

        if context["params"].get("fail_recompute"):
            raise RuntimeError(
                "recompute failed deliberately (fail_recompute=true) -- "
                "the teardown must still lift the embargo"
            )

        source = RawFilesSource()
        done: list[dict] = []
        con = duckdb.connect(str(resolve_warehouse().path))
        try:
            for i, relation in enumerate(affected, 1):
                result = R.recompute_relation(
                    con, source, relation, run_id=context["run_id"]
                )
                print(f"  {i:>2}/{len(affected)}  RECOMPUTED {result.relation:<26} "
                      f"{result.rows:>9,} rows  {result.seconds}s")
                done.append({
                    "relation": result.relation,
                    "rows": result.rows,
                    "seconds": result.seconds,
                })
        finally:
            con.close()
        print(f"recomputed {len(done)} relation(s) in dependency order")
        return done

    @task(pool="duckdb_writer")
    def verify_report(relation: str, **context) -> dict:
        """Confirm one affected report was rebuilt in this run, and read it.

        Dynamically mapped over the affected reports. These *are* independent of
        each other -- each only reads its own relation -- so mapping is the right
        shape here in a way it was not for the recompute.
        """
        import duckdb
        from branchlin import remediate as R
        from branchlin.prober import resolve_warehouse

        con = duckdb.connect(str(resolve_warehouse().path))
        try:
            rebuilt = R.recomputed_in_run(con, context["run_id"])
            if relation not in rebuilt:
                raise RuntimeError(
                    f"{relation} was embargoed as affected but never recomputed "
                    f"in this run -- refusing to certify it as corrected"
                )
            rows = con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]
        finally:
            con.close()
        print(f"VERIFIED {relation}: rebuilt in this run, {rows:,} rows")
        return {"relation": relation, "rows": int(rows), "verified": True}

    # ---- measure the corrected figure -----------------------------------
    @task(pool="duckdb_writer")
    def measure(**context) -> dict:
        """Read the corrected figure, and the evidence for the approval gate."""
        import duckdb
        from branchlin import remediate as R
        from branchlin.prober import resolve_warehouse

        params = context["params"]
        con = duckdb.connect(str(resolve_warehouse().path))
        try:
            corrected = R.measure(
                con, params["relation"], params["column"], params["selector"]
            )
            recomputed = R.recomputed_in_run(con, context["run_id"])
            held = R.embargoed_relations(con)
        finally:
            con.close()

        original = params.get("original_value")
        try:
            original = float(original) if original not in (None, "", "None") else None
        except (TypeError, ValueError):
            original = None

        evidence = R.ApprovalEvidence(
            relation=params["relation"],
            column=params["column"],
            selector=params["selector"],
            break_relation=params.get("break_relation"),
            break_step=params.get("break_step"),
            break_file=params.get("break_file"),
            blamed_branch=params.get("blamed_branch") or None,
            original_value=original,
            corrected_value=corrected,
            code_version=params.get("code_version") or None,
            code_provenance=params.get("code_provenance") or None,
            recomputed=recomputed,
            embargoed=held,
        )
        body = evidence.render()
        print(body)
        return {
            "corrected_value": corrected,
            "original_value": original,
            "delta": evidence.delta,
            "recomputed": list(recomputed),
            "body": body,
        }

    # ---- GATE: human approval -------------------------------------------
    # Deliberately NOT in the duckdb_writer pool. The gate parks in
    # `awaiting_input` until a human answers, and a parked task holding the
    # single writer slot would block the entire pipeline for as long as nobody
    # was looking. It touches no warehouse, so it needs no slot.
    approval = ApprovalOperator(
        task_id="approve_external_notification",
        subject=(
            "Notify external consumers that {{ params.relation }}."
            "{{ params.column }} was published wrong?"
        ),
        body="{{ ti.xcom_pull(task_ids='measure')['body'] }}",
        # On rejection: skip downstream rather than fail the run. The corrected
        # figures stay, nobody is told, and the teardown still lifts the
        # embargo. Failing here would be misleading -- a human declining to
        # notify is a valid outcome, not an error.
        fail_on_reject=False,
    )

    # ---- notify (mocked) -------------------------------------------------
    @task(pool="duckdb_writer")
    def notify(**context) -> dict:
        """Record that external consumers were told. Sends nothing."""
        import duckdb
        from branchlin import remediate as R
        from branchlin.prober import resolve_warehouse

        params = context["params"]
        measured = context["ti"].xcom_pull(task_ids="measure") or {}
        con = duckdb.connect(str(resolve_warehouse().path))
        try:
            R.notify_external(
                con,
                run_id=context["run_id"],
                subject=(
                    f"Restatement: {params['relation']}.{params['column']} "
                    f"for {params['as_of']}"
                ),
                body=measured.get("body", ""),
                payload={
                    "relation": params["relation"],
                    "column": params["column"],
                    "selector": params["selector"],
                    "original_value": measured.get("original_value"),
                    "corrected_value": measured.get("corrected_value"),
                    "delta": measured.get("delta"),
                    "break_relation": params.get("break_relation"),
                    "break_step": params.get("break_step"),
                    "code_version": params.get("code_version"),
                },
            )
            sent = R.notifications_for(con, context["run_id"])
        finally:
            con.close()
        print(f"NOTIFICATION RECORDED (mock, nothing sent): {sent}")
        return {"notifications": len(sent)}

    # ---- TEARDOWN: lift the embargo, whatever happened -------------------
    @task(pool="duckdb_writer")
    def lift_embargo(**context) -> list[str]:
        """Release the embargo. Runs on success or failure."""
        import duckdb
        from branchlin import remediate as R
        from branchlin.prober import resolve_warehouse

        con = duckdb.connect(str(resolve_warehouse().path))
        try:
            released = R.lift_embargo(con, run_id=context["run_id"])
            still_held = R.embargoed_relations(con)
        finally:
            con.close()
        print(f"EMBARGO LIFTED on {len(released)} relation(s): "
              f"{', '.join(released) if released else 'nothing'}")
        print(f"still embargoed by other runs: "
              f"{', '.join(still_held) if still_held else 'nothing'}")
        return list(released)

    # ---- wiring ----------------------------------------------------------
    held = embargo()
    affected = blast_radius()
    reports = affected_report_list()
    fixed = correction()
    rebuilt = recompute(affected)
    verified = verify_report.expand(relation=reports)
    measured = measure()
    sent = notify()
    lift = lift_embargo()

    held >> affected >> fixed >> rebuilt >> verified >> measured >> approval >> sent
    affected >> reports

    # as_teardown(setups=...) is what makes the lift run even when a task in
    # between fails, and what stops the run being marked successful merely
    # because the cleanup worked.
    held >> lift.as_teardown(setups=held)
    sent >> lift


remediate()
