# de-branch-lineage

**Branch-level lineage for Airflow.** Every lineage tool on the market is
column-level: it will tell you that `net_amount` derives from `amt_net`. None of
them tell you *which branch* a row took — which `CASE` arm evaluated, which
`WHERE` predicate excluded it, which join missed.

That gap is documented by the incumbent. dbt's column-level lineage
documentation states that it reflects lineage from `SELECT` statements and
"doesn't reflect other usage like joins and filters". Both bugs planted in this
project are a join and a filter.

> **Status: Phase 1.** The pipeline, the two planted bugs and the verification
> harness are complete and reproducible, and **DIAGNOSE is implemented** — it
> finds both planted bugs, names the branch responsible, and stays quiet on a
> clean warehouse. 96 tests. REMEDIATE, the human approval gate and any UI are
> Phase 2 and are not built. See [docs/STATUS_TECHNICAL.md](docs/STATUS_TECHNICAL.md)
> and [docs/BUILD_LOG.md](docs/BUILD_LOG.md).

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

**REMEDIATE** (Phase 2, not built) — embargo downstream publishing, walk the asset graph to find the
blast radius, recompute in dependency order, then pause at a human approval gate
before notifying external consumers that a published figure was wrong. Teardown
lifts the embargo whether the run succeeded or failed.

The SQL parsed during diagnosis comes from the DAG bundle version that *actually
ran* on the date in question — not from today's repo. That is the argument for
why this belongs in an orchestrator rather than a standalone tool: only the
orchestrator knows which code produced which run.

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
.venv/bin/python -m pytest tests/branchlin       # the DIAGNOSE engine, 77 tests

astro dev start                                  # Airflow UI
astro dev pytest tests/                          # everything, in the image: 96
```

The six version-pinning tests skip inside the image: Astro bind-mounts
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
