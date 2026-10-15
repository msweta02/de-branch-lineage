# Technical reference

This is reference material for engineers reviewing, running or extending
de-branch-lineage. Every figure here was measured by running the code. The
command that produces each one is given so it can be re-derived. For the design
itself, see [architecture.md](architecture.md).

- [Runtime](#runtime)
- [The synthetic warehouse](#the-synthetic-warehouse)
- [Verified figures](#verified-figures)
- [Branch points on the BUG A path](#branch-points-on-the-bug-a-path)
- [What DIAGNOSE finds](#what-diagnose-finds)
- [Tests](#tests)
- [The dbt fixture](#the-dbt-fixture)
- [Known issues](#known-issues)
- [Limits of the engine](#limits-of-the-engine)
- [Operational notes](#operational-notes)

---

## Runtime

| Component | Version | Notes |
|---|---|---|
| Astro Runtime | `3.3-7` | image about 1.5 GB |
| Airflow | 3.3.1+astro.4 | read from the image label, not the tag |
| Task SDK | 1.3.1+astro.2 | |
| Python (image) | 3.14.7 | |
| Python (host venv) | 3.14.4 | `.venv/`, for the seed generator, runner, harness and library tests |
| DuckDB | 1.5.5 | warehouse and query engine |
| sqlglot | 30.18.0 | SQL parser |
| pandas | 2.3.3 | pinned to the version the image ships, deliberately not 3.x |
| pytest | 9.1.1 | host only; the image ships its own |
| dbt-core / dbt-duckdb | 1.12.4 / 1.11.0 | `.venv-dbt/`, **not** in the Airflow image |

**Why Airflow 3.3.** On 3.1 and 3.2 a human-in-the-loop operator defers onto
the triggerer, so the approval gate would depend on a healthy triggerer. From
3.3 the scheduler parks the task in `awaiting_input` directly.

**Image customisation.** There are three customisations:

- `Dockerfile` sets `PYTHONPATH` so that `branchlin` is importable by name.
- `packages.txt` installs `git`, which version pinning shells out to.
- `docker-compose.override.yml` mounts `.git` read-only into every Airflow
  container.

**Ports.** The UI and API are on 8080, and Postgres is on **5434**
(`.astro/config.yaml`). The Astro CLI also serves the UI on a per-project
hostname, `http://de-branch-lineage.localhost:6563`.

---

## The synthetic warehouse

`include/seed/generate.py` writes a DuckDB file of about 4.5 MB containing raw
landing tables.

| | |
|---|---|
| trades | 54,475 rows |
| gross (`sum(quantity * price)`) | 6,346,061,649.20 |
| business dates | 22, within 2026-08-10 → 2026-09-08 (30 calendar days) |
| books / counterparties / currencies | 12 / 8 / 5 |

`TICKET_SIZE_SCALE = 0.4468` is the single calibration knob. Every measure is
linear in quantity, so the headline figure moves linearly with it. It's applied
as a multiplier rather than by narrowing the `randrange` bounds, because
narrowing the bounds changes how many bits the RNG consumes and reshuffles the
whole book.

**All four scenarios produce a byte-identical `raw.trades`**: the same SHA-256
over all rows, the same count and the same gross. Every delta is therefore
attributable to the planted bug. This needed a fix. The first BUG A
implementation skipped an `rng.random()` call along with the missing rate,
which desynchronised every later draw and produced a different book. Every rate
row is now generated, and the BUG A row is removed afterwards.

**Two traps when editing the generator.** Don't change how many bits the RNG
consumes unless you mean to reshuffle the book. And keep the relaxed
permissions on the file *and* its parent directory: the containers run as uid
50000, the host as your own user, and DuckDB writes a sibling `.wal`.

---

## Verified figures

From `tests/verify_scenarios.py`, which takes about 12 s for all four
scenarios:

| scenario | P&L @ 2026-09-03 | delta | P&L @ 2026-08-31 | delta |
|---|---:|---:|---:|---:|
| healthy | 607,617.49 | — | 685,940.98 | — |
| bug_a | 501,622.18 | **−105,995.31** | 685,940.98 | 0.00 |
| bug_b | 607,617.49 | 0.00 | 663,923.90 | **−22,017.08** |
| both | 501,622.18 | −105,995.31 | 663,923.90 | −22,017.08 |

**Row counts don't find either bug.** Under BUG A every relation holds exactly
the healthy row count at all 20 hops. Only the NULL count moves, and it returns
to zero before the report:

```
relation                 healthy      bug_a
trd.trades_converted   49,508/0   49,508/410
pos.book_positions     14,727/0   14,727/ 96
pos.positions_netted   19,779/0   19,779/197
rpt.pnl_daily           6,138/0    6,138/  0   <- NULLs absorbed by COALESCE
rpt.desk_summary          110/0      110/  0   <- the report looks clean
```

**Neither control catches BUG A.** `rpt.recon_control` reports 0 breaks with
both bugs planted. `ref.validity_window` reports GBP as `FULL_SPAN` even though
`rate_days` is 29 against 30 calendar days: it compares only the minimum and
maximum dates, and never compares the two counts it carries. See
[planted-bugs.md](planted-bugs.md).

---

## Branch points on the BUG A path

Measured with sqlglot over the causal path from `ref.fx_rates` to
`rpt.desk_summary`:

```
ref.fx_rates → trd.trades_converted → trd.trades_classified → pos.book_positions
→ pos.positions_adjusted → pos.positions_marked → pos.positions_accrued
→ pos.positions_netted → rpt.pnl_daily → rpt.regulatory_extract → rpt.desk_summary
```

| | |
|---:|---|
| 11 | relations |
| 10 | hops (edges) |
| 3 | module boundaries crossed |
| 30 | CTE-level steps |
| 50 | `CASE` arms (WHEN + ELSE) |
| 11 | join conditions |
| 8 | `WHERE` conjuncts |
| **69** | reconcilable branch points, as originally counted |
| **70** | including the `QUALIFY` the original count missed |

`SQLSource.count_branch_points` produces these numbers, and
`tests/branchlin/test_parser_counts.py` asserts every one, so they can't drift.
Writing that test found the 70th branch point. A
`QUALIFY ROW_NUMBER() OVER (...) = 1` in the `official_marks` CTE of
`03_positions/03_mark_to_market.sql` is on the path, and the original script
never looked for `QUALIFY`.

The tracer rediscovers this path without being told it. It walks the ten
relations from the report, names `ref.fx_rates` as the origin, and never visits
`ref.validity_window`, which the dependency graph would include.

---

## What DIAGNOSE finds

| Scenario | Traced | Result |
|---|---|---|
| bug_a @ 2026-09-03 | `total_pnl_usd` | BREAK at `trd.trades_converted`, CTE `converted`, blamed on `LEFT JOIN ref.fx_rates` |
| bug_b @ 2026-08-31 | `accrued_interest_usd` | BREAK at `pos.positions_accrued`, CTE `accrued`, blamed on the accrual-window `WHERE` |
| either bug, the other date | either | no break |
| healthy | either | no break, complete path to source |

---

## Tests

| Suite | Count | Where it runs |
|---|---:|---|
| `tests/branchlin/` | 110 | host or image: parser counts, tracer on all scenarios, components, remediation helpers, version pinning |
| `tests/dags/test_dag_integrity.py` | 25 | image: one task per SQL file in a single chain, the writer pool on every SQL task, the Asset chain, safe-mode discovery |
| `tests/dags/test_remediate_dag.py` | 32 | image: DAG shape, then `TestFailurePath` and `TestHappyPath` against live Airflow |
| **total** | **167** | |

| Environment | Result | Why it differs |
|---|---|---|
| host venv | 110 passed | no Airflow, so the DAG tests aren't collected |
| `astro dev pytest tests/` | 145 passed, 22 skipped | one-off container with its own mounts: no `.git`, and an empty SQLite metadata DB |
| **scheduler container** | **167 passed** | the only environment with both `.git` and a migrated metadata DB |

```bash
docker exec -e PYTHONPATH=/usr/local/airflow:/usr/local/airflow/include \
  $(docker ps --format '{{.Names}}' | grep de-branch-lineage | grep scheduler) \
  python -m pytest /usr/local/airflow/tests -q -p no:cacheprovider
```

`TestFailurePath` runs REMEDIATE with `airflow dags test`, which works even when
the DAG is paused. `TestHappyPath` triggers it through the scheduler, so
**`remediate` must be unpaused**, or its 8 tests error after a 180-second wait.

---

## The dbt fixture

`include/dbt_fixture/` is a deliberately small dbt project: four models and two
seeds, kept out of the Airflow image. It reproduces BUG A in compressed form on
dbt's own tooling. With the GBP rate missing, `fct_desk_summary` shows the same
trade count, fewer converted trades, and a `sum()` that is silently short:

```
trade_date   book   trade_count   converted_trade_count   base_amount_usd
2026-09-03   BK01             2                       1          6048.00
2026-09-03   BK02             2                       1         10074.69
```

Nothing in dbt's column-level lineage points at the join. See
[the fixture's README](../include/dbt_fixture/README.md) to run it.

---

## Known issues

1. **Version pinning fails inside an Airflow task.** `branchlin.pinning`
   resolves `DagRun.bundle_version` and the recorded XCom through the Airflow
   ORM. Airflow 3.3.1 forbids that in task processes:

   ```
   resolution: lookup failed: RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0
   code version: working tree at /usr/local/airflow/include/sql (UNPINNED -- not the code that ran)
   ```

   This was observed on 2026-10-14, even after building the warehouse through
   the four pipeline DAGs. The trace labels the result `UNPINNED` rather than
   hiding it, and resolution works when `pinning` runs outside a task. The
   orchestration test accepts `UNPINNED` as a valid outcome, so it doesn't catch
   this. **Fix:** read the version through the Task SDK (XCom pulls by
   `dag_id`/`run_id`) or the REST API.
2. **DIAGNOSE passes an empty `correction_sql` by default.** When DIAGNOSE
   triggers REMEDIATE, it forwards its own `correction_sql` param, which
   defaults to `""`, meaning "already fixed". REMEDIATE's built-in BUG A
   correction is therefore only used when REMEDIATE is triggered directly. Pass
   the correction explicitly, as
   [running-the-demo.md](running-the-demo.md#5-scenario-b-bug-a-diagnosed-and-repaired)
   shows.
3. **New DAGs start paused.** Unpause `diagnose` and `remediate` (and the
   pipeline DAGs, if you use them) after the first `astro dev start`.
4. **`astro dev pytest` can't write its cache directory**
   (`Permission denied: /pytest-cache-files-*`). The tests still pass. Pass
   `-p no:cacheprovider` to silence it.
5. **Float-sign noise in the harness.** A delta of exactly zero prints as
   `-0.00` on some runs, because DuckDB's aggregation order isn't
   deterministic. Adding `+ 0.0` before formatting would fix the sign.

## Limits of the engine

1. **CTE-level evidence is static.** A CTE isn't materialised, so there's
   nothing to probe. A break is localised to a CTE by analysis and confirmed at
   the relation that contains it. Re-executing a CTE as a standalone `SELECT`
   would fix this.
2. **No near-miss detection.** EXPLAIN reports `ELSE` arms taken and outer joins
   that NULL-extended. "This row was one day inside the window" needs item 1.
3. **Pushdown widens coarsely.** Window, aggregate and `CASE` selectors widen to
   the whole relation rather than the partition. That's safe, but it does
   unnecessary work.
4. **`sources/dbt.py` is a stub, by decision.** The interface is in place, and
   the fixture above exists to test it against.
5. **REMEDIATE's recompute bypasses the pipeline DAGs.** It re-executes the
   affected statements from `include/sql/` directly, over one connection, in
   dependency order. A general-purpose recompute would re-run the owning
   pipeline tasks instead.

## Operational notes

1. **Approving the gate needs the API or the UI.** Writing
   `hitl_detail.chosen_options` and setting the task to `scheduled` looks
   equivalent but fails:
   `execute_complete() missing 1 required positional argument: 'event'`.
   Use `PATCH /api/v2/dags/{dag}/dagRuns/{run}/taskInstances/{task}/-1/hitlDetails`.
2. **From inside the containers the API server is `http://api-server:8080`.**
   The `AIRFLOW__API__BASE_URL` of `http://localhost:8080` is correct only for a
   browser on the host.
3. **The orchestration tests use the real warehouse.** The DAG resolves it, so
   the tests can't be handed a private copy. Both test classes restore a healthy
   warehouse afterwards.
4. **`duckdb_writer` is a hard constraint.** Every task that opens the
   warehouse must name the pool, including read-only ones. A declared pool has
   no effect until a task names it.
