# Architecture

This page explains how de-branch-lineage fits together: the pipeline it
diagnoses, the `branchlin` library that does the diagnosis, and the two Airflow
DAGs that put it in front of an operator. It also records why each piece is
shaped the way it is.

![de-branch-lineage architecture](images/architecture.png)

*Source: [diagrams/architecture.svg](diagrams/architecture.svg).*

---

## 1. The pieces

| Piece | Location | Role |
|---|---|---|
| Seed generator | `include/seed/generate.py` | Writes the raw landing tables of a synthetic trading book, healthy or with either bug planted |
| Pipeline SQL | `include/sql/` | 20 files, one statement each, in four modules |
| Pipeline DAGs | `dags/0*_pipeline.py`, built by `include/pipeline_dag.py` | One DAG per module, chained by Airflow Assets |
| Runner | `include/runner.py` | Runs the same 20 files in dependency order without Airflow |
| `branchlin` | `include/branchlin/` | The library that diagnoses and remediates |
| DIAGNOSE | `dags/diagnose.py` | Runs a trace from operator-supplied inputs |
| REMEDIATE | `dags/remediate.py` | Repairs, recomputes and publishes behind an approval gate |
| Warehouse | `include/data/warehouse.duckdb` | DuckDB; generated, never committed |

### The pipeline

The pipeline is deliberately production-shaped and too large to trace by eye:

```
01_reference   calendar, FX rates, book hierarchy, counterparty, FX coverage window
02_trades      ingest, dedupe, amend, enrich, convert to USD, classify
03_positions   aggregate by book, corporate actions, mark-to-market, accrual, netting
04_reporting   daily P&L, regulatory extract, desk summary, recon control
```

Every SQL file declares its output and inputs in a header. That header is the
dependency graph that `runner.py` and `branchlin.sources.rawfiles` order by.
Inside a pipeline DAG, the factory in `include/pipeline_dag.py` runs one task
per file, in filename order:

```sql
-- target: trd.trades_converted
-- depends: trd.trades_enriched, ref.fx_rates, ref.validity_window
```

| DAG | Tasks | Triggered by |
|---|---:|---|
| `01_reference_pipeline` | 5 | cron `0 2 * * *` |
| `02_trades_pipeline` | 6 | Asset `reference_layer` |
| `03_positions_pipeline` | 5 | Asset `trade_layer` |
| `04_reporting_pipeline` | 4 | Asset `position_layer` |

### The warehouse

| Schema | Contents |
|---|---|
| `raw` | Landing tables written by the seed generator |
| `ref` | Calendar, FX rates, book hierarchy, counterparties, FX coverage window |
| `trd` | Trades: ingested → deduped → amended → enriched → converted → classified |
| `pos` | Positions: by book → adjusted → marked → accrued → netted |
| `rpt` | Daily P&L, regulatory extract, desk summary, recon control |
| `meta` | Seed metadata (`seed_run`, `pipeline_flags`). REMEDIATE adds `publication_embargo`, `remediation_log` and `external_notifications` on first use |

DuckDB accepts **one writer at a time**, and its file lock refuses even
read-only connections while a writer holds it. Every task that opens the
warehouse, including DIAGNOSE's read-only trace, therefore holds the
single-slot `duckdb_writer` pool. The pool is defined in `airflow_settings.yaml`.
`max_active_runs=1` isn't enough on its own: it serialises the runs of one DAG,
not of four, and the Asset chain is exactly what starts two DAGs at once.

---

## 2. The `branchlin` library

| Module | Responsibility |
|---|---|
| `sources/__init__.py` | The `SQLSource` interface: list statements, walk the dependency graph, count branch points |
| `sources/rawfiles.py` | `SQLSource` over `include/sql/`, ordered by the `-- depends:` headers |
| `sources/dbt.py` | Reserved for a dbt manifest source. Intentionally a stub |
| `parser.py` | sqlglot → branch points: `CASE` arms, `WHERE`/`HAVING` conjuncts, join conditions (flagged for NULL-extension), `QUALIFY` |
| `pushdown.py` | Carries the row selector upstream through each statement, and finds which input columns feed a measure |
| `prober.py` | Narrow, read-only point queries: one value, a row count and a NULL count per probe. Never `SELECT *` |
| `reconciler.py` | Decides whether a hop balances, and blames a CTE and branch point when it doesn't |
| `tracer.py` | The walk: INVESTIGATE halts at the first break, EXPLAIN walks to source |
| `pinning.py` | Works out which commit produced a run, and reads the SQL at that commit |
| `remediate.py` | Blast radius, recompute order, correction target, embargo, evidence |

### How a diagnosis works

The input is four values: a relation, a selector identifying the row, the
measure that's wrong, and the as-of date.

```python
from branchlin.prober import Prober
from branchlin.sources.rawfiles import RawFilesSource
from branchlin.tracer import Tracer

with Prober() as prober:
    trace = Tracer(RawFilesSource(), prober).investigate(
        "rpt.desk_summary", "business_date = DATE '2026-09-03'", "total_pnl_usd",
    )
print(trace.render())
```

At each hop the tracer goes through five steps:

1. **Parse** the statement that produced the relation, splitting it into its
   CTE steps and branch points.
2. **Push the selector down.** It rewrites "this report row" into the
   equivalent rows of each input. If a window, aggregate or `CASE` makes that
   impossible to do precisely, it widens the selector to the whole relation.
   Wider is safe, because it still contains the answer.
3. **Follow the measure.** `pushdown.measure_inputs` finds which input columns
   actually carry the figure. The walk goes there, not to every declared
   dependency. That's why it never visits `ref.validity_window`, which is a
   declared input of `trd.trades_converted` but joins on currency alone and so
   can't transmit a per-date fault.
4. **Probe** the output and each input with narrow read-only queries.
5. **Reconcile.** The reconciler runs five checks: the selector matched rows,
   no NULLs appeared where the inputs had none, sums tie where the hop is a pure
   sum, row counts behave, and types are stable.

| Verdict | Meaning |
|---|---|
| `BALANCED` | The hop's output accounts for its inputs, so the fault is upstream |
| `APPROXIMATE` | It balances, but the selector had to be widened to reach it |
| `BREAK` | The first hop that doesn't balance: the root cause |
| `UNTRUSTED` | Upstream of the break. Not proven wrong, but reached only through broken logic |

**The check that finds both planted bugs is NULL introduction, not arithmetic.**
Under BUG A every hop has the same row count as a healthy run, and the
pipeline's recon control ties. The signal that moves is a NULL appearing in a
column whose inputs had none. At the break, the reconciler attributes the
failure to the CTE and branch point that can produce it. For BUG A that's the
`LEFT JOIN ref.fx_rates` in CTE `converted`.

---

## 3. DIAGNOSE

`dags/diagnose.py` has three tasks:

```
investigate  →  break_found  →  trigger_remediate
```

- `investigate` runs an INVESTIGATE trace from the DAG params (`relation`,
  `selector`, `column`, `as_of`). It prints the rendered trace to the task log
  and returns a verdict to XCom: the break relation, CTE, file, blamed branch,
  untrusted set and original value. It holds the `duckdb_writer` pool.
- `break_found` is a `ShortCircuitOperator`. It continues only if the caller set
  `trigger_remediate: true` **and** the verdict is `BREAK`. A diagnosis is read
  far more often than it's acted on, so remediation is opt-in.
- `trigger_remediate` starts REMEDIATE, passing the break and the
  `correction_sql` param through as conf.

---

## 4. REMEDIATE

`dags/remediate.py`:

| Task | Pool | What it does |
|---|---|---|
| `embargo` (setup) | `duckdb_writer` | Marks the affected reports not-for-publication in `meta.publication_embargo` |
| `blast_radius` | default | Walks forward over the `-- depends:` graph from the corrected table |
| `affected_report_list` | default | The reports to verify, for the mapped task |
| `correction` | `duckdb_writer` | Applies the operator-supplied data fix (`correction_sql`) |
| `recompute` | `duckdb_writer` | Rebuilds the blast radius in dependency order, in **one** task |
| `verify_report` | `duckdb_writer` | Dynamically mapped, one instance per affected report |
| `measure` | `duckdb_writer` | Reads the corrected figure and renders the evidence |
| `approve_external_notification` | default | `ApprovalOperator`: waits in `awaiting_input` |
| `notify` | `duckdb_writer` | **Mock**: writes a `MOCK` row, sends nothing |
| `lift_embargo` (teardown) | `duckdb_writer` | Lifts this run's embargo |

The shape encodes four properties, and each one is covered by a test:

- **The gate can't be bypassed.** `notify`'s only upstream is the gate, so a
  rejection skips the notification.
- **The gate doesn't hold the writer slot.** It may wait for hours, and holding
  the single DuckDB slot would block the whole warehouse. It touches no data, so
  it runs on the default pool.
- **The teardown's trigger rule is `ALL_DONE_SETUP_SUCCESS`.** It runs however
  the upstream finished, but only if the embargo was actually placed.
- **A failed repair is reported as failed.** A successful cleanup doesn't mask
  it.

### Why the recompute is one task, not a mapped one

The recompute was first written with dynamic task mapping. Mapped instances
aren't scheduled in map-index order (one run started at index 7), so
`rpt.desk_summary` was rebuilt from inputs that hadn't been rebuilt yet. Every
task went green, and the "corrected" figure came back as the original wrong
number. A single pool slot serialises execution, but it doesn't order it.
Mapping is now used only for `verify_report`, whose instances really are
independent.

### Why the recompute starts where the fix lands

The BUG A correction writes to `raw.fx_rates`, but the break is three relations
later, at `trd.trades_converted`. Starting the recompute at the break would skip
`ref.fx_rates`, the table the broken join actually reads, and restate nothing.
`remediate.correction_target` therefore parses the correction SQL with sqlglot
to find the table it writes, and the blast radius starts there. For BUG A that's
13 relations, beginning at `ref.fx_rates`.

### The embargo is a table, not a lock

`meta.publication_embargo` can be queried with a plain `SELECT`. "Is this figure
safe to publish?" is answerable at any moment, including mid-remediation and
after a failure, and the read path works over a read-only connection.

---

## 5. Which code ran: version pinning

A diagnosis should parse the SQL that **actually ran** on the date in question.
Only the orchestrator knows which code produced which run, which is the core
argument for building this inside Airflow.

Airflow 3.3 can do this natively: a git-backed DAG bundle records a commit SHA
on every DagRun. But serving DAGs from a git bundle means no change is live
until it's committed and pushed, which is a real cost during development. The
project therefore uses a **hybrid**:

- The pipeline runs from the local `dags-folder` bundle.
- Each SQL task records `git rev-parse HEAD` to XCom. XCom rather than
  `DagRun.conf`, because on Airflow 3.3 a task can't write its own run's conf.
- `branchlin.pinning` resolves a version in four steps, most authoritative
  first:
  1. `DagRun.bundle_version`
  2. a `code_version` passed in conf at trigger time
  3. the recorded XCom
  4. the working tree, labelled **`UNPINNED`**
- It reads the SQL at the resolved commit with `git archive`, which never writes
  to the object database.

Two pieces of plumbing are needed:

- `git` in `packages.txt`, because the runtime image ships none.
- `docker-compose.override.yml`, which mounts `.git` read-only into the
  containers. Astro only bind-mounts `dags`, `include`, `plugins` and `tests`.

> **Current limitation.** When DIAGNOSE runs as an Airflow task, steps 1 and 3
> need the metadata database, and Airflow 3.3.1 forbids ORM access from task
> processes (`RuntimeError: Direct database access via the ORM is not allowed
> in Airflow 3.0`). Live runs therefore fall through to `UNPINNED`. They say so,
> rather than presenting today's code as the code that ran. Resolution works when
> `pinning` is called outside a task. The fix is to read the version through the
> Task SDK or the REST API instead of the ORM.

---

## 6. Design decisions

| Decision | Why |
|---|---|
| Faults are planted in **data**, not SQL | The SQL is byte-identical in every scenario. That's how real failures arrive, and the parser sees production-shaped SQL, not feature flags |
| The walk follows the **measure**, not the dependency graph | The graph has five paths from `ref.fx_rates` to the report. Only one carries the amount |
| Walk at **CTE** granularity, report at **relation** granularity | 30 CTE steps locate the fault precisely. The 10 relation hops are what a person reads |
| `LEFT JOIN` in the FX conversion | An inner join would delete the trade. A vanished trade is worse than an unvalued one, and it's also what makes BUG A silent |
| Remediation is **opt-in** from DIAGNOSE | A diagnosis is read far more often than it's acted on |
| The external notification is **mocked and gated** | It's the one irreversible action in the project |
| **No custom UI** | Airflow's Required Actions view already shows the evidence and the Approve/Reject buttons. The effort went into proving the failure path instead |
| DuckDB as the warehouse | The whole demo runs on a laptop with one command. The `SQLSource` and `Prober` seams are where a real warehouse would plug in |
