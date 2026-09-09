# Technical status

**As of 2026-09-08 · Phase 0 complete · Phase 1 (DIAGNOSE) implemented**

> Sections 1-4 describe Phase 0 and remain accurate. Section 5 is superseded:
> the `branchlin` library is no longer a set of stubs. See section 8 and
> docs/BUILD_LOG.md, "Phase 1".

Audience: engineers picking this up, reviewing it, or building Phase 1 on it.
Every figure below was measured by running the code, not estimated. The
commands that produce each one are given so they can be re-derived.

---

## 1. Summary

Phase 0 is complete: a production-shaped data pipeline, two planted bugs that
survive their own controls, and a verification harness that proves the bugs are
measurable and attributable. This is the **test fixture** the product will be
built against.

The product itself — the `branchlin` library — is **not implemented**. All seven
modules under `include/branchlin/` are docstrings reserving the interface: 125
lines total, zero executable statements beyond `__version__ = "0.0.0"`.

Read that split carefully before planning work on top of this repo. What exists
is a rig that can prove a diagnosis engine works. The diagnosis engine does not
exist.

---

## 2. Runtime

| Component | Version | Notes |
|---|---|---|
| Astro Runtime | `3.3-7` | image size 1.52 GB |
| Airflow | 3.3.1+astro.4 | version read off the image label, not the tag |
| Task SDK | 1.3.1+astro.2 | |
| Python (image) | 3.14.7 | |
| Python (host venv) | 3.14.4 | `.venv/`, used by seed/runner/harness |
| DuckDB | 1.5.5 | warehouse and query engine |
| sqlglot | 30.18.0 | parser dependency, installed but unused so far |
| pandas | 2.3.3 | pinned to the version the image ships, deliberately not 3.0.x |
| dbt-core / dbt-duckdb | 1.12.4 / 1.11.0 | `.venv-dbt/`, **not** in the Airflow image |

**Why Airflow 3.3 specifically.** REMEDIATE needs a human approval gate. On
Airflow 3.1/3.2 a HITL operator defers onto the **triggerer**, making the gate
dependent on a healthy triggerer process. From 3.3 the scheduler parks the task
in `awaiting_input` directly. This is the one pin that is load-bearing for a
feature not yet built; backporting to 3.1/3.2 changes the approval gate and
nothing else.

Postgres is pinned to **5434** in `.astro/config.yaml` (5432 was bound on the
Windows host, invisible from inside WSL). Astro CLI 1.45 fronts the UI on a
per-project hostname (`http://de-branch-lineage.localhost:6563`) as well as
8080.

---

## 3. What is built

### 3.1 The pipeline

Four modules, 20 SQL files, one statement per file, executed as four
Asset-wired Airflow DAGs (20 tasks total).

```
01_reference   calendar, FX rates, book hierarchy, counterparty, coverage window
02_trades      ingest, dedupe, amend, enrich, convert to USD, classify
03_positions   aggregate, corporate actions, mark-to-market, accrual, netting
04_reporting   daily P&L, regulatory extract, desk summary, recon control
```

Every file declares its own dependencies in a header comment:

```sql
-- target: trd.trades_converted
-- depends: trd.trades_enriched, ref.fx_rates, ref.validity_window
```

`include/runner.py` parses those headers today to order execution outside
Airflow. `branchlin.sources.rawfiles` is specified to read the same headers in
Phase 1 — though its own docstring names the Airflow task and asset graph as the
ordering source (rejecting filename prefixes as a dependency declaration), so
header-derived and graph-derived ordering will need to agree. See §6.2.

Asset wiring, verified by loading the DagBag in-container:

| DAG | tasks | schedule |
|---|---:|---|
| `01_reference_pipeline` | 5 | cron `0 2 * * *` |
| `02_trades_pipeline` | 6 | asset `reference_layer` |
| `03_positions_pipeline` | 5 | asset `trade_layer` |
| `04_reporting_pipeline` | 4 | asset `position_layer` |

### 3.2 The synthetic warehouse

`include/seed/generate.py` produces a 4.5 MB DuckDB file of raw landing tables.

| | |
|---|---|
| trades | 54,475 rows |
| gross (`sum(quantity * price)`) | 6,346,061,649.20 |
| business dates | 22 (within a 30-calendar-day window, 2026-08-10 → 2026-09-08) |
| books / counterparties / currencies | 12 / 8 / 5 |

`TICKET_SIZE_SCALE = 0.4468` is the single calibration knob. Every measure in
the pipeline is linear in trade quantity, so the headline figure moves linearly
with it. It is applied as a *multiplier* rather than by narrowing the
`randrange` bounds, because narrowing the range changes how many bits the RNG
consumes and reshuffles the entire book.

### 3.3 The two planted bugs

Both are toggled from the generator. **The SQL is byte-identical in the healthy
and broken states** — the code doesn't change, the reference data does.

```bash
.venv/bin/python include/seed/generate.py            # healthy
.venv/bin/python include/seed/generate.py --bug-a    # join failure
.venv/bin/python include/seed/generate.py --bug-b    # filter failure
```

**BUG A — a JOIN failure.** The GBP rate for 2026-09-03 is *absent* from
`raw.fx_rates` — exactly one row removed, and zero null rates anywhere in the
table. This distinction is what defeats the control: `02_load_fx_rates.sql:25`
filters `WHERE rate IS NOT NULL`, which rejects a null but cannot reject a row
that never arrived.

The coverage control in `05_ref_validity_window.sql` then passes it through. Its
`CASE` compares only `min(rate_date)`/`max(rate_date)` against the calendar
bounds. Verified under BUG A:

```
ccy   covered_from  covered_to  rate_days  calendar_days  coverage_status
GBP   2026-08-10    2026-09-08         29             30  FULL_SPAN
```

The control computes `rate_days` and `calendar_days`, carries both as output
columns, and never compares them. The evidence sits in an adjacent column and
the status still reads `FULL_SPAN`.

Downstream: the LEFT JOIN in `05_convert_currency.sql` misses → `base_amount`
goes NULL → `NULL >= 25000000` is NULL not TRUE, so the `CASE` in
`06_classify_product.sql` falls to its ELSE arm → `SUM()` in
`03_mark_to_market.sql` skips the NULLs → the shortfall lands in
`rpt.desk_summary` with no null, no warning and no missing row on the report.

**BUG B — a FILTER failure.** `ref.calendar.accrual_period_end` is seeded as the
last day of the month instead of the first of the next:

| calendar_date | healthy `accrual_period_end` | bug_b |
|---|---|---|
| 2026-08-31 | 2026-09-01 | **2026-08-31** |
| 2026-09-01 | 2026-10-01 | **2026-09-30** |

`04_accrue_interest.sql:50-51` consumes it as a half-open interval
(`>= start AND < end`), which is correct SQL against a correct calendar. The
excluding `WHERE` lives inside an `accrual_basis` CTE that positions are LEFT
JOINed to, so position rows survive and only the accrual is lost — the basis CTE
drops 150 → 145 rows, and the report comes back light on one day a month in a
column nobody reconciles.

An earlier version applied the filter inline, which deleted the whole day from
`rpt.desk_summary` (110 rows → 105). A missing day in a daily report is noticed
immediately, which defeats the purpose.

### 3.4 The verification harness

`tests/verify_scenarios.py` rebuilds and reruns the pipeline under all four bug
combinations and reports the headline figure, supporting evidence, and row/null
counts at every hop. Runtime ~12s for all four.

```
scenario   pnl @ 2026-09-03        delta    pnl @ 2026-08-31        delta
healthy          607,617.49            —          685,940.98            —
bug_a            501,622.18  -105,995.31          685,940.98         0.00
bug_b            607,617.49         0.00          663,923.90   -22,017.08
both             501,622.18  -105,995.31          663,923.90   -22,017.08
```

**Attribution is sound.** All four scenarios produce a byte-identical
`raw.trades` — same SHA-256 over all rows, same 54,475 count, same gross — so
each delta is attributable to the bug and nothing else. This was not free: the
first implementation of BUG A `continue`d out of the FX generation loop, which
also skipped an `rng.random()` call, desynchronising every subsequent draw. The
healthy and broken runs produced *different trades* (54,475 vs 54,502) and the
"delta" was measuring a different book. Fixed by generating every rate row and
removing the BUG A row as a post-filter.

### 3.5 The dbt fixture

`include/dbt_fixture/` — dbt-core 1.12.4 + dbt-duckdb 1.11.0, four models, two
seeds, out of the Airflow image. It exists to demonstrate the incumbent's gap on
the incumbent's own tooling, and to be the second `SQLSource` implementation's
test target.

BUG A reproduces in compressed form. With the GBP rate for 2026-09-03 missing,
two trades find no rate and `fct_desk_summary` shows:

```
trade_date   book   trade_count   converted_trade_count   base_amount_usd
2026-09-03   BK01             2                       1          6048.00
2026-09-03   BK02             2                       1         10074.69
```

Same trade count, fewer converted, and `sum()` silently short. Nothing in dbt's
column-level lineage points at the join.

### 3.6 Hop distance on the BUG A causal path

Measured with sqlglot over the causal path from `ref.fx_rates` to
`rpt.desk_summary`, on the longest single-line path (11 relations, 10 edges):

| | |
|---|---:|
| relations | 11 |
| edges (hops) | 10 |
| module boundaries crossed | 3 |
| CTE-level steps | 30 |
| CASE arms (WHEN + ELSE) | 50 |
| join conditions | 11 |
| WHERE conjuncts | 8 |
| **total reconcilable branch points** | **69** |

`ref.fx_rates → trd.trades_converted → trd.trades_classified →
pos.book_positions → pos.positions_adjusted → pos.positions_marked →
pos.positions_accrued → pos.positions_netted → rpt.pnl_daily →
rpt.regulatory_extract → rpt.desk_summary`

**Now reproduced by committed code.** `SQLSource.count_branch_points` counts
them and `tests/branchlin/test_parser_counts.py` asserts every figure above, so
they can no longer drift silently. That was the point of landing the counter as
library code rather than leaving it in a throwaway script.

**And writing that test corrected one of the figures.** There is a `QUALIFY` on
the causal path — `ROW_NUMBER() OVER (...) = 1` in the `official_marks` CTE of
`03_positions/03_mark_to_market.sql` — which the original script never looked
for. The three categories in the table still sum to 69; the true total is
**70**. Both numbers are pinned by tests. The table is right about what it
counts and was wrong about what there was to count.

**The tracer rediscovers this path unaided.** It walks the ten relations above
plus `ref.fx_rates` as the named origin, following the measure rather than the
dependency edges, and never visits `ref.validity_window`. See section 8.

---

## 4. Verification evidence

Everything in this table was run on 2026-09-08 against commit `b5e8748`.

| Check | Command | Result |
|---|---|---|
| Seed generator | `include/seed/generate.py` (×4 combos) | 54,475 rows, identical SHA-256 across all four |
| Pipeline runner | `include/runner.py` | 20/20 SQL files, ~2s |
| Scenario harness | `tests/verify_scenarios.py` | all four scenarios, figures as above, exit 0 |
| Row-count invariance | harness hop table | identical row counts across all four scenarios at all 20 hops |
| Recon control | `rpt.recon_control` | **0 breaks** with both bugs planted |
| Coverage control | `ref.validity_window` | GBP `FULL_SPAN` despite `rate_days` 29 vs 30 |
| Branch-point counts | throwaway sqlglot script | 11/10/50/11/8/69/30, matching the README exactly |
| DAG registration | in-container DagBag | 4 DAGs, 20 tasks, **0 import errors** |
| DAG integrity tests | `astro dev pytest tests/dags` | **25 passed** |
| Airflow bring-up | `astro dev start` | 5 containers; UI HTTP 200 on both 8080 and `:6563` |
| Writer serialisation | `airflow pools list` | `duckdb_writer`, 1 slot, assigned on every SQL task |
| dbt fixture | `dbt seed && dbt run` | 2 seeds + 4 models pass; BUG A reproduces |

**Row counts do not find either bug.** Under BUG A every relation holds exactly
the same number of rows as the healthy run at all 20 hops. Only the null count
moves — and it returns to zero by the time it reaches the report:

```
relation                 healthy      bug_a
trd.trades_converted   49,508/0   49,508/410
pos.book_positions     14,727/0   14,727/ 96
pos.positions_netted   19,779/0   19,779/197
rpt.pnl_daily           6,138/0    6,138/  0   <- nulls gone
rpt.desk_summary          110/0      110/  0   <- report looks clean
```

---

## 5. What is NOT built

> **Superseded by section 8.** This section described the state at Phase 0 and
> is kept for the record. Every module in the table below except
> `sources/dbt.py` is now implemented, and two modules not listed here
> (`pushdown.py`, `pinning.py`) were added.

`include/branchlin/` **was** stub modules with docstrings only at Phase 0 — the
interface reserved, no behaviour.

| Module | Intended responsibility | State |
|---|---|---|
| `parser.py` | sqlglot → branch points (CASE arms, WHERE/HAVING conjuncts, joins incl. outer null-extension, QUALIFY/window dedupe) | stub |
| `tracer.py` | DIAGNOSE entry point; backward walk in reverse DAG order, halting at first imbalance | stub |
| `prober.py` | narrowest read-only point queries per hop, connections via Airflow's registry | stub |
| `reconciler.py` | per-hop balance verdict; exonerate balanced hops, mark upstream of a break untrusted | stub |
| `sources/__init__.py` | abstract `SQLSource` interface | stub |
| `sources/rawfiles.py` | `SQLSource` over `include/sql/` | stub |
| `sources/dbt.py` | `SQLSource` over `manifest.json` + `target/compiled/` | stub |

Also not built:

- **The DIAGNOSE DAG.** `dags/` contains only the four pipeline DAGs.
- **The REMEDIATE DAG** — embargo, blast-radius walk, recompute in dependency
  order, HITL approval gate, teardown that lifts the embargo on success *or*
  failure. None of it exists. This is what the Airflow 3.3 pin is for.
- **Bundle-version-aware SQL retrieval.** The design requires parsing SQL from
  the DAG bundle version that actually ran on the date under investigation,
  rather than from the working tree. This is the core argument for living inside
  an orchestrator, and it is unimplemented and unprototyped. Treat it as the
  main technical risk in Phase 1 — everything else is a parser and some queries.

---

## 6. Known issues and open decisions

1. ~~**Hop-granularity decision, unresolved.**~~ **Settled.** Walk at CTE
   granularity (30 steps), report at relation granularity (10 hops), quote 69
   branch points as the size of the search space. One caveat discovered while
   implementing it: a CTE is not materialised, so CTE-level reasoning is static
   and evidence is gathered at relation boundaries. See section 8.
2. ~~**`branchlin.sources.rawfiles` has two conflicting ordering specs.**~~
   **Settled.** The `-- depends:` headers are the single source of truth,
   matching `runner.py`; the docstring has been corrected. Filename prefixes now
   break ties between genuinely independent statements and do nothing else.
3. **`astro dev pytest` cannot write its cache directory** (`Permission denied:
   /pytest-cache-files-*`). Tests pass; the warning is noise.
4. **Cosmetic float-sign noise in the harness headline.** A delta that is
   exactly zero to the cent prints as `-0.00` on some runs and `0.00` on others,
   because DuckDB's aggregation order is not deterministic across runs. Adding
   `+ 0.0` before formatting would pin the sign.
5. **`duckdb_writer` is a hard constraint, not a tuning knob.** DuckDB accepts a
   single writer. `max_active_runs=1` serialises runs of *one* DAG and does
   nothing across four, and the asset chain is precisely the mechanism that gets
   two running at once. Every SQL task must name the pool. A declared pool has
   no effect until a task names it.

---

## 7. Reproducing this from scratch

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

.venv/bin/python include/seed/generate.py        # healthy warehouse
.venv/bin/python include/runner.py               # run all four modules
.venv/bin/python tests/verify_scenarios.py       # all four bug combinations

astro dev start                                  # Airflow UI, ports 8080 + 5434
astro dev pytest tests/dags                      # 25 DAG integrity tests
```

`verify_scenarios.py` re-seeds in place per scenario and leaves the warehouse in
the **`both`** state. Re-seed before using it as a healthy baseline, or the DAGs
will read a doubly-broken warehouse.

Two traps if you touch the generator: relax file **and** parent-directory
permissions on the warehouse (the containers run as uid 50000, the generator as
uid 1000, and DuckDB writes a sibling `.wal`), and never change how many bits
the RNG consumes unless you intend to reshuffle the whole book.


---

## 8. Phase 1 -- the DIAGNOSE engine (implemented)

Seven modules, ~1,900 lines of library code, 77 tests. Run with
`.venv/bin/python -m pytest tests/branchlin` on the host, or
`astro dev pytest tests/branchlin` in the image (the six pinning tests skip
there -- `.git` is not mounted into the containers).

| Module | State |
|---|---|
| `parser.py` | sqlglot to branch points: CASE arms, WHERE/HAVING conjuncts individually, joins flagged for null-extension, QUALIFY |
| `sources/__init__.py` | `SQLSource` interface, graph walking, branch-point counting |
| `sources/rawfiles.py` | implemented over `include/sql/`, ordered by `-- depends:` |
| `sources/dbt.py` | **stub, by decision** -- see its docstring |
| `pushdown.py` | selector rewriting, measure lineage, widening rules |
| `prober.py` | narrow read-only probes; no `SELECT *` anywhere |
| `reconciler.py` | per-hop verdict and blame attribution to a CTE |
| `tracer.py` | the walk, INVESTIGATE and EXPLAIN |
| `pinning.py` | which code ran, via `git archive` |

### What it proves

| scenario | traced | result |
|---|---|---|
| bug_a @ 2026-09-03 | `total_pnl_usd` | BREAK `trd.trades_converted`, CTE `converted`, blamed on `LEFT JOIN ref.fx_rates` |
| bug_b @ 2026-08-31 | `accrued_interest_usd` | BREAK `pos.positions_accrued`, CTE `accrued`, blamed on the `accrual_basis` WHERE |
| either bug, other date | either | no break |
| healthy | either | no break, 15 relations to source |

The tracer **rediscovers the documented causal path without being told it**: 10
relations walked plus `ref.fx_rates` named as the origin, and it never visits
`ref.validity_window`, which the dependency graph would have included. It
follows the measure, not the edges.

### Caveats worth knowing before Phase 2

1. **CTE-level evidence is static, not measured.** A CTE is not materialised, so
   there is nothing to probe. Breaks are localised to a CTE by analysis and
   confirmed at the relation containing it. Re-executing a CTE as a standalone
   SELECT is the obvious upgrade.
2. **Near-miss predicate detection is not implemented.** EXPLAIN reports ELSE
   arms taken and outer joins that null-extended, both measured. "This row was
   one day inside the window" needs (1).
3. **A DIAGNOSE task must hold the `duckdb_writer` pool slot.** Read-only
   probes are not exempt from DuckDB's file lock -- measured, see BUILD_LOG
   "Surprise 16".
4. **Version pinning is the hybrid, not the native mechanism.** Retrieval is
   proved against real history; what is not yet wired is a pipeline run
   recording its own commit. One line in a Phase 2 task.
