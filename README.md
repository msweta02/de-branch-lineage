# de-branch-lineage

**Branch-level lineage for Airflow.** Every lineage tool on the market is
column-level: it will tell you that `net_amount` derives from `amt_net`. None of
them tell you *which branch* a row took — which `CASE` arm evaluated, which
`WHERE` predicate excluded it, which join missed.

That gap is documented by the incumbent. dbt's column-level lineage
documentation states that it reflects lineage from `SELECT` statements and
"doesn't reflect other usage like joins and filters". Both bugs planted in this
project are a join and a filter.

> **Status: complete.** The pipeline, the two planted bugs and the verification
> harness are reproducible; **DIAGNOSE** finds both bugs, names the branch
> responsible and stays quiet on a clean warehouse; **REMEDIATE** embargoes the
> affected reports, recomputes in dependency order, pauses at a human approval
> gate, and lifts the embargo whether the run succeeded or failed. **167 tests.**
> There is no plugin UI, by decision — the Required Actions tab is the UI. See
> [docs/STATUS_TECHNICAL.md](docs/STATUS_TECHNICAL.md) and
> [docs/BUILD_LOG.md](docs/BUILD_LOG.md).

## What it does

**DIAGNOSE** — given a wrong value in a report, walk backwards through the SQL.
At each hop, check whether the output accounts for its inputs and show which
branch the row took. Stop at the first hop that doesn't balance: that's the root
cause. Everything upstream of the break is marked untrusted, because the trace
derived it from logic that is itself broken.

```python
from branchlin.prober import Prober
from branchlin.sources.rawfiles import RawFilesSource
from branchlin.tracer import Tracer

trace = Tracer(RawFilesSource(), Prober()).investigate(
    "rpt.desk_summary", "business_date = DATE '2026-09-03'", "total_pnl_usd",
)
print(trace.render())
```

On the BUG A warehouse that walks ten relations and stops at the tenth — real
output, trimmed only in width:

```
 9. BREAK  trd.trades_converted  signed_base_amount  value=44,944,661.87  rows=2,243  nulls=410
        02_trades/05_convert_currency.sql   step: converted
        * ELSE 'RATE_NOT_FOUND'  --  conversion_status = 'RATE_NOT_FOUND' for 410 rows
        * LEFT JOIN ref.fx_rates AS f ON p.currency = f.ccy AND p.trade_date = f.rate_date
              --  outer join left 410 NULL in signed_base_amount, rows that found no match
        BREAK: 410 NULL in signed_base_amount (18.3% of 2,243 rows) where inputs had none
               (ref.fx_rates.rate=0/149, trd.trades_enriched.gross_amount_local=0/2,243)
        responsible branch points:
          [converted] LEFT JOIN ref.fx_rates AS f ON p.currency = f.ccy AND p.trade_date = f.rate_date
ROOT CAUSE  trd.trades_converted  in CTE 'converted'  (02_trades/05_convert_currency.sql)
UNTRUSTED   14 relation(s) upstream of the break: trd.trades_enriched, ref.fx_rates, ...
```

Note what the last two lines are doing. The break is localised to a **CTE**, not
just to a file — `converted` is the step holding the join that failed. And the
fourteen relations upstream are marked untrusted rather than clean: the trace
would have had to pass through the broken join to reach any of them, so their
evidence cannot be relied on either way.

It runs in two modes. **INVESTIGATE** halts at the first break, for when a
number is known to be wrong. **EXPLAIN** never halts: it walks to the raw
landing tables annotating the branch each row took, for onboarding, audit, and
"why is this trade classified as large". Same parser, same prober, same
reconciler — only the halt condition differs.

The path is **discovered, not declared.** The walk follows the measure rather
than the `-- depends:` edges, which matters because the dependency graph offers
five paths from `ref.fx_rates` to `rpt.desk_summary` and only one of them can
carry the amount. `ref.validity_window` is a declared input of
`trd.trades_converted` but joins on currency alone, so a trace of the amount
never visits it — a graph walk would report a path two hops longer than the one
the fault actually took.

**REMEDIATE** — embargo downstream publishing, walk the graph to find the blast
radius, recompute in dependency order, then pause at a human approval gate
before notifying external consumers that a published figure was wrong. Teardown
lifts the embargo whether the run succeeded or failed.

```
SETUP     embargo        mark the affected reports not-for-publication
TASK      blast_radius   walk forward from the correction over the -- depends: graph
TASK      correction     apply the operator-supplied data fix
TASK      recompute      re-run the affected relations in dependency order
TASK      verify_report  mapped over the affected reports, one row each
TASK      measure        read the corrected figure, build the evidence
GATE      approval       ApprovalOperator; parks in awaiting_input
TASK      notify         MOCKED — writes a row, sends nothing
TEARDOWN  lift_embargo   runs on SUCCESS or FAILURE
```

The gate is not bypassable: `notify` is downstream of it, so a rejection skips
the notification. Telling an external consumer that a published number was
wrong is the one genuinely irreversible action in the project, which is why it
is mocked *and* gated. Here is what a real parked gate showed:

```
| published (wrong) | 501,622.18 |
| corrected         | 607,617.49 |
| delta             | 105,995.31 |

- break at `trd.trades_converted` in CTE `converted` (02_trades/05_convert_currency.sql)
- code version the original ran under: `b5e8748df0c1` (via run-xcom)
```

That delta is the Phase 0 figure to the cent, reached from the opposite
direction: Phase 0 measured it by planting the bug, Phase 2 by remediating it.

**The failure path is the part worth watching.** Run REMEDIATE with the
recompute rigged to fail and the run is reported failed, the teardown lifts the
embargo anyway, the approval gate is never reached, nothing is notified, and the
reports still hold the original figure rather than a half-recomputed one. A
script would need its own `finally` and would still lose if the process were
killed — and having got the cleanup right, it would then have to avoid
reporting success just because the cleanup worked. That is the argument for an
orchestrator, and it is a test (`TestFailurePath`), not a claim.

### Which code ran

The SQL parsed during diagnosis comes from the commit that *actually ran* on the
date in question — not from today's working tree. That is the argument for why
this belongs in an orchestrator rather than a standalone tool: only the
orchestrator knows which code produced which run.

Airflow 3.3 can do this natively. A git-backed DAG bundle records a commit SHA
on every DagRun, and instantiating the bundle at that SHA materialises the
repository as it was; that was spiked first, because it was the biggest risk in
the project, and it works — including the awkward part, that `include/sql/`
lives outside `dags/` and is still inside the bundle. But serving DAGs from a
git bundle means a code change is not live until it is committed and pushed,
which is a real cost during development.

So the shipped arrangement is a **hybrid**: the pipeline keeps running from the
local `dags-folder` bundle, and each SQL task records its own
`git rev-parse HEAD` per run. `branchlin.pinning` then resolves a version in
four steps, most authoritative first — Airflow's own `DagRun.bundle_version`,
then a `conf['code_version']` supplied at trigger, then the recorded XCom, then
the working tree — and reads the SQL at that commit with `git archive`, which
never writes to the object database. A trace that cannot establish a version
says `UNPINNED` rather than quietly reporting today's code:

```
version_for_run('04_reporting_pipeline', '2026-09-09')
  -> b5e8748df0c1 / run-xcom
```

Two pieces of plumbing make this work and are easy to miss: `git` in
`packages.txt`, because the Astro Runtime image ships no git binary, and
`docker-compose.override.yml`, which mounts `.git` read-only into the
containers — Astro bind-mounts `include`, `tests`, `dags` and `plugins` and
nothing else, so without it the containers hold the working tree with no history
behind it.

## Runtime

| | |
|---|---|
| Astro Runtime | `3.3-7` |
| Airflow | **3.3.1+astro.4** |
| Python | 3.14.7 |
| Task SDK | 1.3.1+astro.2 |
| Warehouse | DuckDB 1.5.5 |

**Why 3.3 and not 3.1.** Human-in-the-loop operators behave differently either
side of 3.3. On Airflow 3.1 and 3.2 a HITL operator defers onto the
**triggerer**, so the approval gate in REMEDIATE depends on a healthy triggerer
process. From 3.3 the scheduler parks the task in an **`awaiting_input`** state
directly. This project is built against the 3.3 behaviour. Backporting to 3.1 or
3.2 would change the approval gate and nothing else.

## Quickstart

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

.venv/bin/python include/seed/generate.py        # healthy warehouse
.venv/bin/python include/runner.py               # run all four modules
.venv/bin/python tests/verify_scenarios.py       # all four bug combinations

.venv/bin/pip install -r requirements-dev.txt    # pytest, for the library tests
.venv/bin/python -m pytest tests/branchlin       # the library, 110 tests

astro dev start                                  # Airflow UI
astro dev pytest tests/                          # in the image: 145, 22 skipped
```

**To run all 167 with nothing skipped**, use the scheduler container. It is the
only environment with both the `.git` mount and a migrated metadata database, so
it is the only one that can exercise version pinning and the two orchestration
paths:

```bash
docker exec -e PYTHONPATH=/usr/local/airflow:/usr/local/airflow/include \
  $(docker ps --format '{{.Names}}' | grep scheduler) \
  python -m pytest /usr/local/airflow/tests -q
```

`astro dev pytest` skips 22 of them. It runs a one-off container that uses the
built image but brings its own mounts, so it has no `.git`, and it falls back to
an empty SQLite metadata database. Both skips are deliberate and reported.

The six version-pinning tests need git history: Astro bind-mounts
`include`, `tests`, `dags` and `plugins`, so the containers have the working
tree but no git history to pin against.

`verify_scenarios.py` re-seeds the warehouse in place for each scenario, so it
leaves it in the `both` state — both bugs planted. Re-run the seed generator and
`runner.py` before using the warehouse as a healthy baseline, or the DAGs in the
Airflow UI will read a doubly-broken warehouse.

`astro dev start` needs ports 8080 and 5434. This project pins Postgres to 5434
in `.astro/config.yaml` because 5432 is commonly taken.

## The pipeline

Four modules, 20 SQL files, ~50,000 trades over 22 business dates, 12 books, 8
counterparties, 5 currencies. Each module is one Airflow DAG; the modules are
wired to each other with Assets, so reporting waits on positions waits on trades
waits on reference.

```
01_reference   calendar, FX rates, book hierarchy, counterparty, coverage window
02_trades      ingest, dedupe, amend, enrich, convert to USD, classify
03_positions   aggregate, corporate actions, mark-to-market, accrual, netting
04_reporting   daily P&L, regulatory extract, desk summary, recon control
```

Every `.sql` file is one statement, with its output and inputs declared in a
header comment that both `include/runner.py` and (in Phase 1)
`branchlin.sources.rawfiles` read:

```sql
-- target: trd.trades_converted
-- depends: trd.trades_enriched, ref.fx_rates, ref.validity_window
```

## The two planted bugs

Both are toggled from the seed generator, so the pipeline can be shown healthy
and then broken **without editing a line of SQL**. The SQL is byte-identical in
both states — which is how these failures actually arrive: the code didn't
change, the reference data did.

```bash
.venv/bin/python include/seed/generate.py --bug-a     # join failure
.venv/bin/python include/seed/generate.py --bug-b     # filter failure
```

### BUG A — a JOIN failure

The GBP rate for 2026-09-03 is missing from `raw.fx_rates`. Note *missing*, not
null: a null would have been rejected by the not-null check in
`02_load_fx_rates.sql`. A row that never arrived cannot be rejected.

```
01_reference/02_load_fx_rates      GBP rate for 2026-09-03 never lands
        ↓                          the coverage control checks min/max dates
01_reference/05_ref_validity_window   only, so a hole in the middle passes
        ↓
02_trades/05_convert_currency      LEFT JOIN misses → base_amount = NULL
        ↓                          (LEFT, so no row is lost — only the value)
02_trades/06_classify_product      NULL >= 25000000 is NULL, not TRUE, so the
        ↓                          CASE falls through to its ELSE arm
03_positions/03_mark_to_market     SUM() silently skips the NULLs
        ↓
04_reporting/03_desk_summary       −105,995.31 USD, with no null, no warning
                                   and no missing row anywhere on the report
```

**Eleven relations, ten hops, three module boundaries, and 69 reconcilable
branch points** (50 CASE arms, 11 join conditions, 8 WHERE conjuncts) between
the missing rate and the wrong figure, spread over 30 CTE-level steps. Counted
with sqlglot over the causal path, not estimated — and now counted by
`branchlin.sources.SQLSource.count_branch_points`, with
`tests/branchlin/test_parser_counts.py` asserting every figure so none of them
can drift.

Writing that test corrected one of them. There is also a `QUALIFY` on the causal
path — `ROW_NUMBER() OVER (...) = 1` in the `official_marks` CTE of
`03_positions/03_mark_to_market.sql` — which the original count never looked
for. The three categories above still sum to 69; the true total is **70**. The
tracer rediscovers this same eleven-relation path on its own, without being
given it.
`04_reporting/04_recon_control` — the control that exists to catch this — passes
clean, because it reconciles trade *counts* (which tie exactly, since no row is
lost) and reconciles amounts against a total that is itself downstream of the
break.

### BUG B — a FILTER failure

`ref.calendar.accrual_period_end` is seeded as the last day of the month instead
of the first day of the next one. The accrual window in
`03_positions/04_accrue_interest.sql` is half-open (`>= start AND < end`), which
is correct SQL against a correct calendar, so it silently excludes every trade
booked on the last calendar day of a month.

Positions are LEFT JOINed to the accrual basis rather than filtered by it, so
the rows survive and the day does not vanish from the report — it just comes
back light, on one day a month, in a column nobody reconciles.

One join, one filter: deliberately the two things dbt's column-level lineage
documents that it does not cover.

## Verified figures

`tests/verify_scenarios.py` rebuilds and reruns the pipeline under all four
combinations. All four produce a byte-identical `raw.trades` (54,475 rows), so
every delta below is attributable to the bug and nothing else.

| scenario | P&L @ 2026-09-03 | delta | P&L @ 2026-08-31 | delta |
|---|---:|---:|---:|---:|
| healthy | 607,617.49 | — | 685,940.98 | — |
| bug_a | 501,622.18 | **−105,995.31** | 685,940.98 | 0.00 |
| bug_b | 607,617.49 | 0.00 | 663,923.90 | **−22,017.08** |
| both | 501,622.18 | −105,995.31 | 663,923.90 | −22,017.08 |

**Row counts do not find either bug.** Under BUG A every relation holds exactly
the same number of rows as the healthy run, at every one of the twenty hops. The
only thing that moves is the null count — and it reaches zero again by the time
it gets to the report.

## Licence

Apache-2.0.
