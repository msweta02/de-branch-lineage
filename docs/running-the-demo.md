# Running the demo

This guide sets the project up from a fresh clone. It then plants each bug,
diagnoses it, repairs it, proves the failure path, runs the tests and resets
everything. Every command here was run against the repository as committed.

- [1. Prerequisites](#1-prerequisites)
- [2. Without Airflow (5 minutes)](#2-without-airflow-5-minutes)
- [3. With Airflow](#3-with-airflow)
- [4. Scenario A: a healthy warehouse](#4-scenario-a-a-healthy-warehouse)
- [5. Scenario B: BUG A, diagnosed and repaired](#5-scenario-b-bug-a-diagnosed-and-repaired)
- [6. Scenario C: BUG B, a filter failure](#6-scenario-c-bug-b-a-filter-failure)
- [7. Scenario D: the failure path](#7-scenario-d-the-failure-path)
- [8. Building the warehouse through Airflow](#8-building-the-warehouse-through-airflow)
- [9. Running the tests](#9-running-the-tests)
- [10. Resetting](#10-resetting)
- [11. Troubleshooting](#11-troubleshooting)

---

## 1. Prerequisites

| Tool | Version used | Needed for |
|---|---|---|
| Python | 3.11+ (developed on 3.14) | the host virtualenv |
| Docker | with Compose v2 | Airflow |
| [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) | 1.45 | `astro dev start` |
| git | any | cloning; the containers read history through a read-only mount |

Airflow needs ports **8080** (UI and API) and **5434** (Postgres) to be free.
Postgres is pinned to 5434 in `.astro/config.yaml` because 5432 is often taken.

```bash
git clone https://github.com/msweta02/de-branch-lineage.git
cd de-branch-lineage
```

---

## 2. Without Airflow (5 minutes)

You can do all of the following with nothing but Python: build the warehouse,
plant the bugs, measure them and run a diagnosis.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt

.venv/bin/python include/seed/generate.py      # healthy raw tables
.venv/bin/python include/runner.py             # run all 20 SQL files in order
```

### Measure both bugs

```bash
.venv/bin/python tests/verify_scenarios.py
```

This rebuilds the warehouse four times (healthy, BUG A, BUG B, both) and prints
the headline figure for each, plus row and NULL counts at every hop:

```
scenario   pnl @ 2026-09-03        delta    pnl @ 2026-08-31        delta
healthy          607,617.49            —          685,940.98            —
bug_a            501,622.18  -105,995.31          685,940.98         0.00
bug_b            607,617.49         0.00          663,923.90   -22,017.08
both             501,622.18  -105,995.31          663,923.90   -22,017.08
```

> It leaves the warehouse in the **`both`** state. Re-seed before you use it as
> a healthy baseline (see [Resetting](#10-resetting)).

### Plant a bug yourself

```bash
.venv/bin/python include/seed/generate.py --bug-a     # missing FX rate (join failure)
.venv/bin/python include/seed/generate.py --bug-b     # wrong month-end (filter failure)
.venv/bin/python include/seed/generate.py --bug-a --bug-b
.venv/bin/python include/runner.py                    # always rerun the pipeline after seeding
```

The SQL doesn't change between scenarios. Only the seeded data does.

### Run a diagnosis from Python

With BUG A planted:

```bash
PYTHONPATH=include .venv/bin/python - <<'EOF'
from branchlin.prober import Prober
from branchlin.sources.rawfiles import RawFilesSource
from branchlin.tracer import Tracer

with Prober() as prober:
    trace = Tracer(RawFilesSource(), prober).investigate(
        "rpt.desk_summary", "business_date = DATE '2026-09-03'", "total_pnl_usd",
    )
print(trace.render())
EOF
```

It ends with:

```
ROOT CAUSE  trd.trades_converted  in CTE 'converted'  (02_trades/05_convert_currency.sql)
```

### Library tests

```bash
.venv/bin/python -m pytest tests/branchlin      # 110 passed
```

---

## 3. With Airflow

```bash
astro dev start
```

This builds the image and starts five containers: Postgres, scheduler,
dag-processor, triggerer and api-server. It also loads `airflow_settings.yaml`,
which creates the `duckdb_warehouse` connection, the single-slot `duckdb_writer`
pool and the `branchlin_sql_root` variable. Open **http://localhost:8080**.
Locally there is no login.

Most commands below run inside the scheduler container. Set this once per shell:

```bash
S=$(docker ps --format '{{.Names}}' | grep de-branch-lineage | grep scheduler)
```

**Unpause the two product DAGs.** Airflow creates new DAGs paused, and a
triggered run of a paused DAG just waits in `queued`:

```bash
docker exec $S airflow dags unpause diagnose
docker exec $S airflow dags unpause remediate
```

> **Seed inside the container once Airflow is running.** The containers run as
> uid 50000 and the host as your own user. Seeding and running the pipeline on
> the same side avoids permission errors on the DuckDB file.

---

## 4. Scenario A: a healthy warehouse

DIAGNOSE should find nothing, and should not start REMEDIATE.

```bash
docker exec $S bash -c "cd /usr/local/airflow && python include/seed/generate.py && python include/runner.py"
docker exec $S airflow dags trigger diagnose -r demo_healthy
```

In the UI open **diagnose → demo_healthy → investigate → Logs**. The trace walks
14 hops back to `raw.trades` and ends:

```
NO BREAK    complete path to source, 14 hops
VERDICT: NO BREAK -- complete path to source over 14 hops
```

`trigger_remediate` is **skipped**.

---

## 5. Scenario B: BUG A, diagnosed and repaired

### Plant the bug

```bash
docker exec $S bash -c "cd /usr/local/airflow && python include/seed/generate.py --bug-a && python include/runner.py"
```

`rpt.desk_summary.total_pnl_usd` for 2026-09-03 now reads **501,622.18**.

### Diagnose, and hand off to REMEDIATE

DIAGNOSE passes its `correction_sql` param through to REMEDIATE, and an empty
string there means "already fixed". So when DIAGNOSE triggers REMEDIATE, pass
the correction explicitly. The command below takes the BUG A correction straight
from `dags/remediate.py`, so it can't be mistyped:

```bash
docker exec $S bash -c 'cd /usr/local/airflow && python -c "
import json, sys; sys.path.insert(0, \"dags\")
from remediate import DEFAULT_CORRECTION
print(json.dumps({\"trigger_remediate\": True, \"correction_sql\": DEFAULT_CORRECTION}))
" > /tmp/conf.json && airflow dags trigger diagnose -r demo_bug_a --conf "$(cat /tmp/conf.json)"'
```

The correction redelivers the missing rate:

```sql
INSERT INTO raw.fx_rates (rate_date, ccy, quote_ccy, rate, rate_source, ingested_at)
SELECT DATE '2026-09-03', 'GBP', 'USD', 1.270854, 'LATE_REDELIVERY', now()
WHERE NOT EXISTS (SELECT 1 FROM raw.fx_rates WHERE rate_date = DATE '2026-09-03' AND ccy = 'GBP')
```

You can also do this from the UI: **diagnose → Trigger**, tick
`trigger_remediate`, and paste the SQL above into `correction_sql`.

The `investigate` log ends with:

```
ROOT CAUSE  trd.trades_converted  in CTE 'converted'  (02_trades/05_convert_currency.sql)
VERDICT: BREAK at trd.trades_converted in CTE 'converted' (02_trades/05_convert_currency.sql)
         responsible: LEFT JOIN ref.fx_rates AS f ON p.currency = f.ccy AND p.trade_date = f.rate_date
```

### Approve

A few seconds later a **remediate** run appears. It works through the repair
and stops at `approve_external_notification` in state **Awaiting Input**.

1. Open **Browse → Required Actions** (or the run's
   `approve_external_notification` task → **Required Action** tab).
2. Check the evidence. It shows published **501,622.18**, corrected
   **607,617.49**, delta **105,995.31**, the root cause and the recompute order.
3. Click **Approve**.

`notify` records a mock notification, `lift_embargo` releases the four reports,
and the run finishes **success**. The report now reads **607,617.49**.

Clicking **Reject** instead skips `notify`. The corrected figures stay in place,
no one is notified, and the embargo is still lifted.

To approve without the UI, use the REST endpoint that the tests use:
`PATCH /api/v2/dags/remediate/dagRuns/{run_id}/taskInstances/approve_external_notification/-1/hitlDetails`
with `{"chosen_options": ["Approve"]}`. Setting the task state in the database
directly doesn't work, because the resume needs the event payload this endpoint
attaches.

---

## 6. Scenario C: BUG B, a filter failure

BUG B surfaces on 2026-08-31, in the accrued interest:

```bash
docker exec $S bash -c "cd /usr/local/airflow && python include/seed/generate.py --bug-b && python include/runner.py"
docker exec $S airflow dags trigger diagnose -r demo_bug_b --conf '{
  "relation": "rpt.desk_summary",
  "selector": "business_date = DATE '\''2026-08-31'\''",
  "column":   "accrued_interest_usd",
  "as_of":    "2026-08-31"
}'
```

Expected verdict:

```
ROOT CAUSE  pos.positions_accrued  in CTE 'accrued'  (03_positions/04_accrue_interest.sql)
VERDICT: BREAK at pos.positions_accrued in CTE 'accrued' (03_positions/04_accrue_interest.sql)
         responsible: WHERE p.calendar_date >= p.accrual_period_start ...
```

REMEDIATE's built-in correction is the BUG A fix, so this scenario stops at the
diagnosis.

---

## 7. Scenario D: the failure path

This scenario shows that a repair which dies part-way leaves things clean. It
triggers REMEDIATE directly, with the recompute rigged to fail:

```bash
docker exec $S bash -c "cd /usr/local/airflow && python include/seed/generate.py --bug-a && python include/runner.py"
docker exec $S airflow dags trigger remediate -r demo_failure --conf '{"fail_recompute": true, "original_value": 501622.18}'
```

Expected task states:

| Task | State |
|---|---|
| `embargo`, `blast_radius`, `affected_report_list`, `correction` | success |
| `recompute` | **failed** |
| `verify_report`, `measure`, `approve_external_notification`, `notify` | upstream_failed |
| `lift_embargo` | **success** |
| the run | **failed** |

You can confirm in the warehouse that nothing was left half-done. Stop or wait
for any running task first, because DuckDB holds an exclusive lock:

```bash
.venv/bin/python -c "
import duckdb; c = duckdb.connect('include/data/warehouse.duckdb', read_only=True)
print(c.sql(\"SELECT round(sum(total_pnl_usd), 2) FROM rpt.desk_summary WHERE business_date = DATE '2026-09-03'\").fetchone())
print(c.sql('SELECT relation, embargoed, lifted_at IS NOT NULL AS lifted FROM meta.publication_embargo').fetchall())
"
```

The report still holds the original **501,622.18**, not a half-recomputed
mixture. Every embargo row shows `embargoed = False` with a `lifted_at`.

---

## 8. Building the warehouse through Airflow

The scenarios above build the warehouse with `runner.py`, which is fast and
deterministic. You can build it through the four pipeline DAGs instead:

```bash
docker exec $S bash -c "cd /usr/local/airflow && python include/seed/generate.py --bug-a"
for d in 01_reference_pipeline 02_trades_pipeline 03_positions_pipeline 04_reporting_pipeline; do
  docker exec $S airflow dags unpause $d
done
docker exec $S airflow dags trigger 01_reference_pipeline
```

`01_reference_pipeline` finishing updates the `reference_layer` Asset. That
starts `02_trades_pipeline`, and so on down to `04_reporting_pipeline`. The
whole chain takes about two minutes.

- `01_reference_pipeline` has a daily schedule, so unpausing it can also create
  one scheduled run. You may see the chain run twice. That's harmless, because
  the writer pool serialises them.
- Each SQL task records its git commit to XCom. A diagnosis run inside Airflow
  still reports `UNPINNED` for now: see the known issue in
  [technical-reference.md](technical-reference.md#known-issues).
- Pause the pipeline DAGs again afterwards if you don't want the daily schedule.

---

## 9. Running the tests

| Where | Command | Result |
|---|---|---|
| Host | `.venv/bin/python -m pytest tests/branchlin` | 110 passed |
| Astro test container | `astro dev pytest tests/` | 145 passed, 22 skipped |
| **Scheduler container** | see below | **167 passed** |

The scheduler container is the only environment with both the `.git` mount and
a migrated metadata database, so it's the only one that runs everything:

```bash
docker exec -e PYTHONPATH=/usr/local/airflow:/usr/local/airflow/include $S \
  python -m pytest /usr/local/airflow/tests -q -p no:cacheprovider
```

The full run takes about four minutes. Before you start it:

- **`remediate` must be unpaused.** `TestHappyPath` triggers it through the
  scheduler. If it's paused, the run waits in `queued` and those 8 tests error
  with *"the approval gate never parked in awaiting_input"*.
- **The orchestration tests use the real warehouse.** The DAG resolves it, so a
  private copy can't be passed in. Both test classes restore a healthy warehouse
  on the way out.
- **`astro dev pytest` skips 22 tests on purpose.** It runs a one-off container
  with its own mounts, so it has no `.git` and only an empty SQLite metadata
  database.

---

## 10. Resetting

**Healthy baseline.** Use whichever side you have been seeding from:

```bash
docker exec $S bash -c "cd /usr/local/airflow && python include/seed/generate.py && python include/runner.py"
# or, without Airflow:
.venv/bin/python include/seed/generate.py && .venv/bin/python include/runner.py
```

**Stop Airflow** (it keeps its metadata database):

```bash
astro dev stop
```

**Start from nothing.** This deletes all Airflow state, including run history:

```bash
astro dev kill
rm -f include/data/warehouse.duckdb
```

---

## 11. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| A triggered run sits in `queued` | The DAG is paused. Run `airflow dags unpause <dag>` |
| Tasks never start and the scheduler logs *"non-existent pool"* | `airflow_settings.yaml` wasn't loaded. Restart with `astro dev start`, or run `airflow pools set duckdb_writer 1 "DuckDB single writer"` |
| `astro dev start` fails on a port | 8080 or 5434 is in use, often by another Astro project. Run `astro dev stop` in that project, or change the port in `.astro/config.yaml` |
| `IO Error: Could not set lock on file` | DuckDB allows one writer and no readers alongside it. Wait for the running task, or stop Airflow, before opening the warehouse from the host |
| `Permission denied` on `warehouse.duckdb` | The file was written by the containers (uid 50000). Re-seed from the side you're working on: `generate.py` recreates the file |
| A diagnosis on a "healthy" warehouse finds a break | `verify_scenarios.py` left it in the `both` state. Re-seed (see [Resetting](#10-resetting)) |
| The evidence shows `code version: None (via working-tree)` | Expected for now: see the known issue in [technical-reference.md](technical-reference.md#known-issues) |
| A REMEDIATE run triggered from DIAGNOSE "restates nothing" | `correction_sql` was empty, which means "already fixed". Pass the correction (see [Scenario B](#5-scenario-b-bug-a-diagnosed-and-repaired)) |
| Astro warns that `de-branch-lineage.localhost` is already registered | Another checkout with the same project name registered the hostname. Use http://localhost:8080 |
