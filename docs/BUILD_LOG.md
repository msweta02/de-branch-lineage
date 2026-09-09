# Build log

Running record of what worked, what didn't, and where a version surprised us.
Kept for the README's "what was hard" section.

---

## Phase 0 -- environment, scaffolding, seeded pipeline

### Step 1 -- prerequisite check

| Tool | Needed | Found |
|---|---|---|
| python3 | 3.11+ | 3.14.4 (host), `python3.12` also present |
| docker | daemon up | 29.7.2, daemon responding |
| docker compose | for `astro dev start` | v5.5.0 |
| astro CLI | recent | 1.45.0 |
| git | any | 2.53.0 |
| node | — | absent on Linux |

Nothing had to be installed.

**Surprise 1 -- `node` is missing but `npm` works.** On WSL, `which npm` resolves
to `/mnt/c/Program Files/nodejs/npm`: the *Windows* Node install leaking through
WSL interop. `node.exe` is v24.11.1 but there is no Linux `node` on PATH. Harmless
here (this project contains no JavaScript) but it is the kind of thing that makes a
`node`-dependent tool fail with a confusing error later. Noted, not fixed.

### Step 2 -- runtime pin

**Decision: Astro Runtime `3.3-7` → Airflow `3.3.1+astro.4`, Python 3.14.7,
Task SDK 1.3.1.** Verified by reading `io.astronomer.docker.airflow.version`
off the pulled image, not by trusting the tag name.

Available series at the time of scaffolding, read from the registry:

| Runtime | Airflow | Python | HITL approval gate |
|---|---|---|---|
| `3.0-17` | 3.0.x | 3.12 | n/a |
| `3.1-20` | 3.1.8 | 3.12 | defers onto the **triggerer** |
| `3.2-9`  | 3.2.2 | 3.13 | defers onto the **triggerer** |
| **`3.3-7`** | **3.3.1** | **3.14** | scheduler-managed **`awaiting_input`** |

This matters for REMEDIATE. On 3.1/3.2 a HITL operator yields to the triggerer
and the approval gate depends on a healthy triggerer process; on 3.3+ the
scheduler parks the task in an `awaiting_input` state directly. We are building
against the **3.3 behaviour**. If this project is ever backported to 3.1, the
approval gate is the thing that changes.

Image size 1.52 GB pulled.

### Step 3 -- dependencies

**Surprise 2 -- every version I reached for from memory was stale.** Checked PyPI
directly. Actual latest: sqlglot 30.18.0, duckdb 1.5.5, pandas 3.0.5,
dbt-core 1.12.4, dbt-duckdb 1.11.0.

**Surprise 3 -- pandas is deliberately NOT pinned to latest.** Runtime 3.3-7 ships
`pandas 2.3.3`. Latest on PyPI is `3.0.5`, a major release. Forcing 3.0 into the
Airflow image would re-resolve a dependency the providers were built against, for
no benefit -- pandas is used here only for formatting result sets. Pinned to the
shipped `2.3.3` so the install is a no-op.

Python 3.14 wheel availability was checked before pinning, because the image is
py3.14 and 3.14 is new enough that missing wheels were a real risk. All five
packages publish either `cp314` manylinux wheels or pure-Python wheels. No source
builds, so no compiler needed in the image.

Verified inside the actual image, not just resolved on paper:
`pip install -r requirements.txt` succeeds and `pip check` reports
"No broken requirements found".

dbt is split into `requirements-dbt.txt` and is not installed in the image.

### Step 4 -- seeding the pipeline

20 SQL files across four modules, ~50k trades, 4 Asset-wired DAGs, 20 tasks.
Every file is a single `CREATE OR REPLACE TABLE ... AS` statement with a header
declaring its output and inputs:

```sql
-- target: trd.trades_converted
-- depends: trd.trades_enriched, ref.fx_rates, ref.validity_window
```

Those headers are the dependency declaration, and `branchlin.sources.rawfiles`
will read the same ones in Phase 1. `include/runner.py` parses them today.

**Surprise 4 -- the two planted bugs desynchronised the random number
generator.** BUG A was first implemented by `continue`-ing out of the FX
generation loop, which also skipped the `rng.random()` call below it. Every
subsequent draw shifted, so the healthy and broken runs produced *different
trades* -- 54,475 rows against 54,502 -- and the "delta" between them was
measuring a different book, not a bug. Fixed by generating every rate row and
removing the BUG A row as a post-filter. All four scenarios now produce a
byte-identical `raw.trades` (54,475 rows, gross 6,346,061,649.20), so the
delta is attributable to the bug and nothing else. This was the single most
important correctness fix in Phase 0; without it every number in the demo
would have been noise.

**Surprise 5 -- the realistic calendar collided with BUG B.** The real UK summer
bank holiday in 2026 falls on 31 August, which is exactly the month end BUG B
targets. Seeding it as a holiday meant no trades were booked that day and BUG B
had nothing to exclude. The holiday was removed and the collision documented in
the generator. Trades are now only booked on weekdays, which also made the
`WHERE is_business_day` filter in the regulatory extract a no-op instead of a
28% silent row loss that would have muddied the hop-by-hop row counts.

**BUG B was redesigned once.** The first version applied the accrual window as
an inner filter, which dropped every position dated 2026-08-31 and deleted the
whole day from `rpt.desk_summary` -- 110 rows became 105. A missing day in a
daily report is noticed immediately, and the brief calls for something "nobody
has noticed". The excluding `WHERE` now lives in an `accrual_basis` CTE that
positions are LEFT JOINed to, so the position rows survive and only their
accrual is lost. The row loss is real but localised inside the CTE
(150 basis rows to 145), and the report comes back light on one day a month in
a column nobody reconciles.

**Calibration.** Every measure is linear in trade quantity, so the headline
figure is set by a single constant, `TICKET_SIZE_SCALE`. It is applied as a
multiplier rather than by narrowing the `randrange` bounds, because narrowing
the range changes how many bits the RNG consumes and reshuffles the whole book
-- measured directly: three different ranges gave deltas of 152,321 / 159,568 /
147,578 with no linear relationship to their means. At `0.4468` the BUG A
shortfall lands on 105,995.31.

### Step 5 -- bringing up Airflow

**Surprise 6 -- port 5432 was already bound on the Windows host.** `astro dev
start` failed with `ports are not available: exposing port TCP
127.0.0.1:5432`. Nothing was visible from inside WSL (`ss -ltn` showed nothing,
no containers running) because the conflict is on the Windows side of the WSL
boundary. Fixed with `astro config set postgres.port 5434`, which writes to the
project's `.astro/config.yaml` and therefore travels with the repo. The
webserver port was left at 8080.

Note that Astro CLI 1.45 fronts the UI with a proxy on a per-project hostname
(`http://de-branch-lineage.localhost:6563`) rather than exposing 8080 directly.

**Surprise 7 -- DAGs silently did not register, with no error anywhere.**
`airflow dags list` returned "No data found" and `DagBag.import_errors` was
empty. The dag-processor logged `Found 0 files for bundle dags-folder` while
`docker exec ls` showed all four files present in the container. Two separate
faults, both mine:

1. The DAG files assigned to `01_reference_pipeline`, which is not a valid
   Python identifier -- identifiers cannot begin with a digit.
2. More interestingly, that syntax error was never reported, because Airflow's
   safe-mode DAG discovery only opens a file if its *contents* contain both the
   strings `dag` and `airflow`. The files imported from
   `include.pipeline_dag` and never mentioned `airflow`, so they were filtered
   out before parsing and the syntax error was never reached.

The second is worth writing down: a DAG file that imports its DAG from a helper
module is invisible to Airflow unless the word "airflow" appears somewhere in
it. There is no warning. The DAG files now carry a comment saying so.

**Surprise 8 -- `Asset()` takes name first, not uri.** `Asset("duckdb://...",
name="reference_layer")` raises `TypeError: got multiple values for argument
'name'`. The first positional parameter is `name`. Use keywords.

### Step 6 -- dbt fixture

dbt-core 1.12.4 with dbt-duckdb 1.11.0, in `.venv-dbt`, out of the Airflow
image. Four models plus two seeds; `dbt seed`, `dbt compile` and `dbt run` all
pass first time. The compressed BUG A reproduces: two GBP trades on 2026-09-03
find no rate, `base_amount` is null, and `sum()` skips them.

Nothing fought back here -- the fixture took well under the 30-minute budget.

**Surprise 9 -- DuckDB's single-writer lock breaks the asset chain.** Triggering
`01_reference_pipeline` manually while `02_trades_pipeline` was still running
from the asset cascade failed with:

```
IO Error: Could not set lock on file ".../warehouse.duckdb":
Conflicting lock is held in airflow worker ... (PID 534)
```

`max_active_runs=1` serialises runs of *one* DAG; it does nothing across four
separate DAGs, and the asset chain is exactly the mechanism that gets two of
them running at once. Fixed with a global one-slot pool (`duckdb_writer`,
declared in `airflow_settings.yaml`) applied to every SQL task. The pool had
been declared earlier but never assigned to anything, which is worth its own
note: declaring a pool has no effect at all until a task names it.

Verified by firing all four DAGs within 12 ms of each other -- all four runs
succeeded, and no lock conflict has appeared since.

**Surprise 10 -- the containers cannot write the warehouse by default.** The
Airflow containers run as uid 50000 (`astro`); the seed generator runs as the
host user (uid 1000) and writes mode 644. Every task failed with `Permission
denied` on the bind-mounted DuckDB file. The generator now relaxes the mode on
both the file and its parent directory -- the directory matters too, because
DuckDB writes a sibling `.wal`.

### Verification summary

* `astro dev start` brings up 5 containers; UI returns HTTP 200.
* All four DAGs register, 20 tasks total, correct order and asset wiring.
* Full asset cascade ran end to end: reference (scheduled) -> trades ->
  positions -> reporting, all `asset_triggered`, all success.
* `astro dev pytest tests/dags` -- 25 passed.
* `tests/verify_scenarios.py` reproduces both bugs with exact deltas.

### Still open for Phase 1

* Hop distance on the BUG A causal path, counted with sqlglot: 11 relations,
  10 edges, 30 CTE-level steps, 69 branch points. The brief asked for 15+ hops;
  the 20-file layout cannot give that at *file* granularity on one straight
  line, since reference feeds trades at file 5 of 6. Decide which granularity
  the demo quotes before the tracer is written.
* `astro dev pytest` cannot write its cache directory (`Permission denied:
  /pytest-cache-files-*`). Harmless -- tests pass -- but noisy.

---

## Phase 1 -- the DIAGNOSE engine

### Settled before starting

* **Granularity** (closes the §6.1 open issue). Walk at CTE granularity (30
  steps), report at relation granularity (10 hops), quote 69 branch points as
  the size of the search space. A user sees 10 hops and can expand any one of
  them into its CTE steps.
* **Ordering** (closes the §6.2 open issue). The `-- depends:` headers are the
  single source of truth, matching `runner.py`. The docstring in
  `sources/rawfiles.py` claiming the Airflow task/asset graph supplies ordering
  is wrong and is to be corrected. One mechanism, not two.

### Task 0 -- the version-pinning spike

The core "why an orchestrator" argument is that branchlin parses the SQL that
*actually ran* on the date under investigation, not the SQL in the working tree.
Spiked first, because if it does not work the argument has to change.

**Outcome: works, but only with a git-backed DAG bundle.** Detail below.

**Finding 1 -- the local dev bundle cannot version, by design.**
`LocalDagBundle.supports_versioning` is `False` (as is the `BaseDagBundle`
default). Measured against the running instance, every row is unversioned:

```
dag_run.bundle_version          -> None   (all runs)
dag_version.bundle_version      -> None   (all 8 rows, 2 versions x 4 DAGs)
dag_bundle 'dags-folder'.version-> None
```

So on the current setup there is nothing to pin a trace to. This is not a
misconfiguration; a folder on disk has no history to expose.

**Finding 2 -- Airflow 3.3 does version DAG *structure*, and retains it.**
`dag_version` holds one row per version per DAG and `serialized_dag` holds one
row per `dag_version_id` -- verified, 2 versions each and 2 serialized rows
each. `DagRun.created_dag_version_id` and `TaskInstance.dag_version_id` point
into it. `DagRun.dag_versions` is explicit about the intent: *"when the dag is
in a versioned bundle, we keep the dag version fixed"*. The plumbing carries
`bundle_version` from the bundle through `_create_orm_dags` into `DagModel` and
`DagVersion` at parse time.

**Surprise 11 -- the serialized DAG does not contain the SQL, or even the path
to it.** This looked like a free win: if the retained serialized DAG held the
SQL, historical retrieval would need no git at all. It does not. The
`serialized_dag` blob for `02_trades_pipeline` is 6,030 bytes and contains
neither `CREATE OR REPLACE`, nor `LEFT JOIN`, nor
`05_convert_currency.sql`, nor `/usr/local/airflow/include/sql`. Only the
task_id `convert_currency` survives, and `op_kwargs` is `{}`.

The reason is a detail of how the Phase 0 DAGs are written. The SQL path is a
*Python default argument* of the decorated callable:

```python
def run_sql(path: str = str(sql_path)):
```

Default arguments are not task parameters, so nothing is serialised and nothing
is rendered. `rendered_task_instance_fields` exists and is populated for our
tasks (65 rows), but stores `{"op_args": [], "op_kwargs": {}}` -- empty, for the
same reason. Had the path been passed as an `op_kwarg` it would have been
persisted per task instance, and had the SQL *text* been passed it would have
been persisted too. That is a real alternative mechanism, but it is capped:
`max_num_rendered_ti_fields_per_task` defaults to 30, so only the last 30 runs
per task survive pruning.

**Surprise 12 -- the Astro image has no `git` binary.** `apache-airflow-
providers-git` installs fine (0.4.3 on PyPI, resolves on py3.14) but imports
GitPython, which refuses to load without a `git` executable:
`ImportError: Bad git executable`. A git bundle therefore needs `git` in
`packages.txt` as well as the provider in `requirements.txt` -- two files and an
image rebuild, not one line.

**Surprise 13 -- `.git` is not mounted into the containers.** Astro bind-mounts
`include`, `tests`, `dags`, `plugins` and `airflow_settings.yaml`, and nothing
else. The containers have the working tree but no history whatsoever. For the
spike the history was copied in with `docker cp` (1.1 MB).

**Finding 3 -- with a git bundle, version-pinned retrieval works, and our SQL
is inside the bundle.** This was the decisive test. `include/sql/` lives outside
`dags/`, so the concern was that a bundle scoped to the DAGs folder would not
carry it. It does: the bundle clones the whole repository.

Two commits were chosen to make the result unambiguous -- `b293ac6`, where the
20 SQL files landed, and `48f7df8`, the scaffold commit before they existed:

```
GitDagBundle(tracking_ref='master')            -> .../spike/tracking_repo
    get_current_version() = b5e8748df0c1a323...   include/sql: 20 files
GitDagBundle(version='b293ac6')                -> .../spike/versions/b293ac6
    get_current_version() = b293ac6               include/sql: 20 files  SQL present
GitDagBundle(version='48f7df8')                -> .../spike/versions/48f7df8
    get_current_version() = 48f7df8               include/sql:  0 files  SQL ABSENT
```

The pinned checkouts are separate directories under `versions/<sha>`, and the
scaffold-commit checkout genuinely lacks the SQL. That is the proof the content
is version-pinned rather than the working tree being re-read.

The retrieval path for a historical run is therefore:

```
dag_run(dag_id, logical_date) -> bundle_version (commit sha)
    -> GitDagBundle(name, tracking_ref, version=sha).path
    -> read include/sql/**/*.sql as of that commit
```

Every link is verified except the population of `dag_run.bundle_version` by a
real run under a git bundle, which needs the full configuration to observe.

**Cost, and the reason this is a decision rather than an action.** Serving DAGs
from a git bundle changes the development loop: Airflow would read the DAGs from
a clone, so a code change is not live until it is committed and pushed. With 16
days and a video still to record, that is a real cost. A lighter variant keeps
`dags-folder` serving the DAGs and has the pipeline record `git rev-parse HEAD`
per run, with branchlin reading SQL from a worktree at that sha -- cheaper, and
still Airflow-scoped, but the pin is self-recorded rather than Airflow-native.
Reported for decision; not chosen unilaterally.

**Spike hygiene.** The provider was installed into the running scheduler
container and uninstalled afterwards; the copied history and bundle clones were
deleted. Re-verified after cleanup: 4 DAGs, 20 tasks, 0 import errors. The
`git` binary installed via apt is still present in the running container and
will disappear on the next container recreate; nothing in the repo was changed.

Spike took well under the half-day timebox.

### Task 0 outcome -- the hybrid, chosen

Reported (B) and the hybrid was chosen: the pipeline keeps running from the
local `dags-folder` bundle, so the dev loop is untouched, and
`branchlin.pinning` resolves a code version in three steps -- Airflow's
`DagRun.bundle_version` first, then a self-recorded `conf['code_version']`,
then the working tree, *labelled as unpinned*. A trace never claims a
provenance it does not have.

`pinning.materialise` uses `git archive`, not `git worktree add`. Archive reads
the object database and writes nothing to it, so a diagnosis cannot leave a
worktree, a lock or a detached HEAD behind in the repository it is
investigating. There is a test asserting `git status --porcelain` is
byte-identical before and after a pinned read.

### Tasks 1-7 -- the DIAGNOSE engine

Seven modules, ~1,900 lines, and 77 tests. `branchlin.sources.dbt` is still a
stub, by decision.

The engine finds both planted bugs, names the branch responsible, and stays
quiet on a clean warehouse:

| scenario | date | traced column | result |
|---|---|---|---|
| bug_a | 2026-09-03 | `total_pnl_usd` | BREAK `trd.trades_converted`, CTE `converted`, blamed on `LEFT JOIN ref.fx_rates ON currency, trade_date` |
| bug_a | 2026-08-31 | `accrued_interest_usd` | no break |
| bug_b | 2026-08-31 | `accrued_interest_usd` | BREAK `pos.positions_accrued`, CTE `accrued`, blamed on the `accrual_basis` WHERE conjuncts |
| bug_b | 2026-09-03 | `total_pnl_usd` | no break |
| healthy | either | either | no break, 15 relations to source |

The date-specificity falls out of the mechanism rather than being coded: the
same trace on the other date balances, matching the Phase 0 delta table where
each bug moves exactly one of the two dates.

**Surprise 14 -- the documented "69 branch points" was missing a QUALIFY.**
Landing the counter as library code with a test immediately contradicted the
figure it was supposed to protect. `03_positions/03_mark_to_market.sql` has a
`QUALIFY ROW_NUMBER() OVER (...) = 1` inside its `official_marks` CTE, and that
file *is* on the causal path. The Phase 0 throwaway script never looked for
QUALIFY, so the published breakdown (50 CASE arms + 11 join conditions + 8
WHERE conjuncts = 69) silently excluded it. The true total on the causal path
is **70**.

Rather than quietly restate the headline, `parser.DOCUMENTED_KINDS` names the
three categories the docs enumerate, and the test pins both numbers: the
documented 69 *and* `qualify == 1`, `total == 70`. The figure in the README is
not wrong about what it counts; it was wrong about what there was to count.

**Surprise 15 -- sqlglot 30 renamed a Select arg and `args.get` returns None
for the old name.** Step dependencies came back empty for every FROM clause
while joins resolved fine. The key is `from_`, not `from`; `args.get("from")`
does not raise, it returns None, so the parser produced a confidently empty
answer. A dict-backed AST has no schema, and a renamed key is indistinguishable
from an absent one.

**Surprise 16 -- a read-only probe still needs the writer pool.** Phase 0
serialises writes through a one-slot pool because DuckDB permits a single
writer, and the obvious assumption is that read-only probes are exempt. The
brief said to verify rather than assume, and the assumption is false: DuckDB
locks the *file*, so `duckdb.connect(path, read_only=True)` opened while a
writer holds it fails with `Could not set lock on file ... Conflicting lock is
held`. A DIAGNOSE task must take the same `duckdb_writer` slot as a pipeline
task. `prober.REQUIRES_WRITER_POOL` records the finding in code so the Phase 2
DAG cannot forget it, and a test reproduces the refusal.

**Surprise 17 -- the granularity decision is only half implementable.** The
brief settled on walking at CTE granularity and reporting at relation
granularity, which is the right split. But a CTE is not materialised: there is
no table to probe. So the walk reasons about CTE steps *statically* -- which
step computes the column, which branch points sit in it, which of them can
null-extend -- and gathers *evidence* only at relation boundaries. A break is
localised to a CTE by analysis and confirmed by measurement at the relation
containing it. That is enough to name the `accrual_basis` WHERE, but it is not
the same as measuring inside the CTE, and the difference should be stated
plainly rather than papered over. Measuring inside a CTE would mean
re-executing it as a standalone SELECT -- possible, since the statement is
already parsed, and the obvious Phase 2 upgrade.

**Surprise 18 -- keying probe results by relation silently dropped columns, and
produced a confident false BREAK.** `pos.positions_adjusted` computes
`net_amount_usd` from a CASE over *two* columns of the same input:
`book_positions.net_amount_usd` and `book_positions.net_quantity`. The inputs
dict was keyed by relation, so the second overwrote the first, the null-bearing
column vanished from the evidence, and the hop was reported as the root cause of
BUG A -- four hops early, with a plausible-looking explanation. Keyed by
`(relation, column)` now. A false positive with a good story attached is worse
than a crash.

**Surprise 19 -- "were NULLs introduced here" took three attempts, and the two
failures were both plausible.** This is the check that finds both bugs, so
getting it wrong meant getting everything wrong.

1. *Presence.* If any input carries a NULL, the output's NULLs are inherited.
   Fails on BUG B: `ref.calendar.day_count_from_prev` is NULL at the window
   boundary (a LAG with nothing before it), which "explained" 678 NULLs with
   one, and the guilty hop was exonerated. The trace sailed past it and blamed
   `ref.calendar`.
2. *Counts.* Require the output's NULL count to exceed its inputs'. Fails on
   BUG A: `03_positions/05_net_by_counterparty` expands 14,727 rows into 19,779
   and multiplies existing NULLs along with them, so the count rises with
   nothing wrong. It blamed the netting hop.
3. *Rates.* Compare NULL fractions instead. Fails on BUG A for a subtler
   reason: the expansion is not uniform. Null-bearing rows there have more
   counterparties, so the null fraction legitimately climbs from 14.2% to 22.0%.
   Blamed the netting hop again.

Any row-multiplying hop defeats (2) and (3) in both directions. What finally
worked was (1) *plus* the missing piece: a column consumed under `COALESCE`
cannot transmit a NULL however many it holds, so it must not count as an
explanation. With COALESCE-wrapped inputs excluded, presence is correct for
both bugs. The rate comparison survives as a non-fatal warning, because "more
NULLs than the inputs carry" is worth telling a human even when it is
legitimate.

**Surprise 20 -- the dependency graph offers five paths and only one carries the
fault.** `ref.fx_rates` reaches `rpt.desk_summary` five ways, and the longest
(12 relations, via `ref.validity_window`) is not the documented causal path (11).
`trd.trades_converted` really does declare `ref.validity_window` as an input, so
a graph walk visits it -- but it joins `ON c.currency = w.ccy`, with no date, and
contributes `coverage_status` rather than the amount. It cannot transmit a
per-date FX fault, and Phase 0 confirms it holds 5 identical rows under BUG A.

So the tracer follows the *measure* rather than the edges, and rediscovers the
documented 11-relation path without being told it. This is the clearest
statement of why branch lineage beats a dependency graph: the graph shows what
*could* have carried the fault, which here is wrong by one relation and two
hops. There is a test asserting the trace never visits `ref.validity_window`.

Choosing which input to follow needed care of its own.
`SUM(mtm_pnl_usd * cp_weight)` draws on the weight's whole lineage as well as
the amount's, and the weight carried *more* NULLs than the amount did --
following the larger null count skipped four relations and reported a path the
money never took. Continuity of the measure (an input column of the same name)
beats volume of nulls; only when the measure is renamed does the walk fall back
to the step's FROM lineage.

**What the engine does not do.** Near-miss predicate detection -- "this row was
one day inside the window" -- is specified for EXPLAIN and is not implemented.
It needs CTE-level re-execution (see Surprise 17), and guessing at it from
relation-level evidence would be dishonest. EXPLAIN's default view is the
branches where the row took a non-obvious path: an ELSE arm taken, or an outer
join that null-extended. Both are measured, not inferred.

### Still open after Phase 1

Ordered by what unblocks the most.

1. **Re-executing a single CTE as a standalone SELECT.** Two deferred features
   both depend on it, so it is one decision rather than two: near-miss predicate
   reporting in EXPLAIN ("this row was one day inside the window"), and
   measuring inside a CTE rather than reasoning about it statically
   (Surprise 17). The statement is already parsed, so extracting a CTE and
   running it read-only is mechanically straightforward; the cost is a query per
   expansion, which is why it should be on demand rather than on every hop.
2. **Three pushdown refinements.** All three currently widen to the whole
   relation, which is safe but coarse:
   * a **window** selector should widen to the window's `PARTITION BY` keys, not
     to everything -- only rows in the same partition participated.
   * a **non-equi join** could be re-executed rather than widened away.
   * a **QUALIFY** dedupe should return all candidate rows and mark which
     survived, rather than dropping the predicate.
   These are precision improvements, not correctness fixes. A too-broad selector
   still contains the break, so no trace is wrong without them -- but a
   window-heavy pipeline will do more work than it needs to.
3. **Wiring the recording half of the hybrid.** `branchlin.pinning` resolves and
   retrieves a version; nothing yet writes `conf['code_version']`, so every
   trace today resolves to `working-tree` and says so. One line in a task, but
   it needs `.git` visible inside the containers -- Astro mounts `include`,
   `tests`, `dags` and `plugins` only (Surprise 13), which is also why six
   pinning tests skip under `astro dev pytest`. A `docker-compose.override.yml`
   mounting `.git` read-only fixes both at once.
4. **`astro dev pytest` still cannot write its cache directory.** Unchanged from
   Phase 0. Harmless, still noisy.

---

## Phase 2 -- REMEDIATE and the DIAGNOSE DAG

Scope was cut deliberately before starting: no CTE re-execution, no pushdown
refinements, no near-miss detection, no plugin UI, no generalised blast-radius
recompute. The Required Actions tab is the UI. Recorded here because a reader
should know what was decided against, not just what was built.

### Task 1 -- wiring the version pinning

Phase 1 left the retrieval half of the hybrid working and the recording half
unwired, so every trace resolved to `working-tree` and said so. Closing that gap
took four findings, none of them the "one line" it was scoped as.

**Surprise 21 -- a task cannot write its own run's `conf` on Airflow 3.3.** The
plan was for each pipeline task to record `git rev-parse HEAD` into
`DagRun.conf['code_version']`. It cannot: task execution goes through the API
server, `conf` is set at trigger time, and the Task SDK exposes no endpoint to
mutate it -- there is no such method on the SDK's DagRun and nothing in
`airflow.sdk` writes conf. Checked before substituting rather than after.

The recording therefore lands in **XCom** under the same key. XCom is the
per-run store that does persist, it is visible in the UI next to the task that
wrote it, and it is readable from the DIAGNOSE task. `conf` support is kept for
the case where it genuinely works: a human triggering a run and supplying the
version themselves. `pinning` now resolves in four steps rather than three:
`bundle_version`, then `conf`, then XCom, then the working tree.

**Surprise 22 -- `logical_date` is NULL for exactly the runs this pipeline
uses.** With the XComs written and visible (20 of them, all four DAGs), the
resolver still returned `working-tree`. On Airflow 3 `logical_date` is nullable
and only *scheduled* runs carry one; asset-triggered and manual runs have NULL.
This pipeline runs almost entirely by asset cascade, so a date-based run lookup
matched nothing -- silently, because the whole resolver was wrapped in a broad
`except Exception: return None, WORKING_TREE`. The fallback was honest about the
outcome and hid the cause completely, which is the worst combination.

The lookup now tries `logical_date`, then `run_after`, then `start_date`, then
the most recent successful run -- and that last case is labelled
`run-xcom (latest run, not date-matched)`, because "the code the latest run
used" is a weaker claim than "the code that ran on 2026-09-03" and should not be
allowed to impersonate it. `version_for_run` also takes an optional `trace` list
that records what was tried, so the next silent failure is one argument away
from being visible.

**Surprise 23 -- `astro dev pytest` does not apply
`docker-compose.override.yml`.** The override mounts `.git` into the four
Airflow services, which was supposed to un-skip the six pinning tests. It did
not: `astro dev pytest` uses the built *image* (so the `git` binary from
`packages.txt` is present) but brings its own mounts, and `.git` is absent.
Diagnosed by making a probe test assert its own environment --
`{'dot_git_exists': False, 'git_binary': '/usr/bin/git', 'git_available': False}`.

So the six tests still skip under `astro dev pytest`, correctly, and they *run*
under the scheduler container, which does have the mount:

```bash
docker exec -e PYTHONPATH=/usr/local/airflow:/usr/local/airflow/include \
  <scheduler> python -m pytest /usr/local/airflow/tests/branchlin -q
# 77 passed
```

**Two smaller traps on the way.** `branchlin` was not importable from a task:
Airflow puts `/usr/local/airflow` on `sys.path`, which is why the DAG files can
`from include.runner import ...`, but the library's modules import each other
absolutely, so it has to be reachable as `branchlin` itself. Fixed with
`ENV PYTHONPATH` in the Dockerfile rather than in `.env`, because `.env` is
gitignored and a setting the pipeline depends on has to travel with the repo.

And git refused to read the mounted repository at all --
`fatal: detected dubious ownership` -- because the containers run as uid 50000
and the bind-mounted repo is owned by uid 1000. Same root cause as Surprise 10,
different symptom. Fixed with a per-invocation `-c safe.directory=<repo>` inside
`pinning`, which is scoped to one subprocess and names one exact path rather
than putting `*` in global config. The project's own test for "reading history
does not mutate the repository" had made the same assumption and had to be
routed through the same helper.

**Verified.** After a full asset cascade:

```
version_for_run('04_reporting_pipeline', '2026-09-09')
  -> b5e8748df0c1 / run-xcom
  trace: ['matched on run_after 2026-09-09',
          "XCom 'code_version' on asset_triggered__2026-09-09T02:54:47..."]
```

A trace now names the commit its SQL came from instead of admitting it does not
know.

### Tasks 2-5 -- the DIAGNOSE and REMEDIATE DAGs

Two DAGs, one new library module (`branchlin.remediate`), 90 new tests. Final
counts: **167 passing in the scheduler container with nothing skipped**; 110 on
the host; 145 passing and 22 skipped under `astro dev pytest`, which has neither
the `.git` mount nor a migrated metadata database.

The shape of REMEDIATE is the deliverable:

```
SETUP     embargo        mark the affected reports not-for-publication
TASK      blast_radius   walk forward from the correction over the -- depends: graph
TASK      correction     apply the operator-supplied data fix
TASK      recompute      re-run the affected relations in dependency order
TASK      verify_report  mapped over the affected reports, one row each
TASK      measure        read the corrected figure, build the evidence
GATE      approval       ApprovalOperator; parks in awaiting_input
TASK      notify         MOCKED -- writes a row, sends nothing
TEARDOWN  lift_embargo   runs on SUCCESS **OR FAILURE**
```

**Surprise 24 -- a recompute is a fold, not a map, and mapping it produced a
confidently wrong answer.** The brief specified dynamic task mapping over the
affected set, which is the obvious shape and reads beautifully in the UI: one
row per relation. It was implemented that way, pinned to a single slot with
`max_active_tis_per_dag=1` on top of the `duckdb_writer` pool, on the assumption
that a single slot would also mean map-index order.

It does not. Mapped instances are scheduled independently -- the first run
started at `map_index=7` -- and the observed rebuild order was:

```
positions_adjusted, positions_netted, desk_summary, positions_accrued,
book_positions, regulatory_extract, trades_classified, positions_marked,
recon_control, pnl_daily
```

`rpt.desk_summary` was rebuilt third, from inputs that had not been rebuilt yet.
Every task succeeded, the run went green, and the "corrected" figure came back
as **501,622.18** -- the original wrong number. Serialising execution is not
ordering it, and a computation whose steps depend on each other cannot be
expressed as a map however few slots it is given.

The recompute is now a single sequential task. Dynamic mapping moved to
`verify_report`, where the units genuinely are independent -- each reads one
relation and nothing else -- so the per-relation UI rows are kept for the part
that can honestly have them.

**Surprise 25 -- the recompute has to start where the fix landed, not where the
break was, and getting this wrong is silent.** Twice, one level apart.

First, `blast_radius` returned strictly-downstream relations, excluding the
break itself. But the break relation is the statement that *consumed* the bad
input, so it is the statement that must consume the corrected input. Excluding
it meant the fix never entered the pipeline and all ten downstream relations
faithfully recomputed the same wrong number.

Fixing that was not enough. The correction inserts into `raw.fx_rates`; the
break is at `trd.trades_converted`, three relations later. In between sit
`ref.fx_rates` and `ref.validity_window`, both *upstream* of the break and so in
nobody's blast radius -- and `ref.fx_rates` is the table the broken LEFT JOIN
actually reads. Rebuilding from the break left the corrected rate sitting in
`raw` where nothing downstream would ever look at it.

Both attempts produced a green run, a completed approval gate, and a delta of
`-0.00`. The failure mode of a remediation is not a crash; it is a restatement
that restates nothing.

The recompute now starts from the relation the *correction* writes to, extracted
from the correction SQL with sqlglot (`correction_target`) rather than taken as
a parameter that can be set wrong. For BUG A that yields 13 relations beginning
at `ref.fx_rates`, and the figure moves 501,622.18 -> **607,617.49**, matching
the healthy pipeline exactly.

**Surprise 26 -- a HITL gate cannot be approved by writing the obvious two
columns.** `hitl_detail` holds `chosen_options`, `responded_at` and
`responded_by`, so approving programmatically for a test looked like a two-column
update plus flipping the task from `awaiting_input` to `scheduled`. The
scheduler duly re-queued the task, which then failed with:

```
TypeError: ApprovalOperator.execute_complete() missing 1 required
positional argument: 'event'
```

The resume path needs the `event` payload the API endpoint attaches; the
endpoint does more than it appears to. The tests now approve through the real
`PATCH .../taskInstances/{task_id}/{map_index}/hitlDetails`, which is better
anyway -- the test exercises exactly what a human clicking **Approve** does.
Note the endpoint is reachable from inside the containers at
`http://api-server:8080`, not at the `AIRFLOW__API__BASE_URL` of
`http://localhost:8080`, which is correct only for a browser on the host.

**Two smaller findings.** A teardown's trigger rule is
`ALL_DONE_SETUP_SUCCESS`, not `ALL_DONE` -- it runs however the upstream
finished, but only if the setup that placed the embargo actually succeeded,
which is the stronger and more correct condition. And the `astro dev pytest`
container has no Postgres configured and silently falls back to an empty SQLite
file, so a `SELECT 1` liveness probe reported a usable metadata database and
eight tests then errored on `no such table: dag_run`. The probe now queries
`dag_run` itself, which is the difference between skipping cleanly and failing
confusingly.

**The failure path, proved (Task 4).** `TestFailurePath` runs REMEDIATE with the
recompute rigged to fail and asserts the eight things that matter:

| assertion | result |
|---|---|
| the run is reported as `failed` | a successful cleanup must not mask a failed remediation |
| `recompute` failed | the rigged failure fired |
| `embargo` was set | the setup ran |
| **`lift_embargo` succeeded anyway** | the teardown is why this works |
| the embargo is lifted *in the warehouse* | not just that the task ran |
| the approval gate was never reached | `upstream_failed`, not `awaiting_input` |
| nothing was notified | the irreversible action did not happen |
| the state left behind is reasonable | reports still hold the original figure, not a half-recomputed one; the remediation log is empty |

That last row is the argument for an orchestrator over a script. A script would
need its own `finally` and would still lose if the process were killed; and
having got the cleanup right, it would have to avoid reporting success just
because the cleanup worked.

**The happy path, proved (Task 5).** `TestHappyPath` seeds BUG A, triggers
REMEDIATE, waits for the gate to park, captures the evidence while it is parked
(it is gone once the run finishes), approves through the API, and asserts the
figure reads 607,617.49, the notification fired exactly once and was a `MOCK`,
the embargo lifted, and the rebuild started at `ref.fx_rates` with the break
relation before the report. Both tests restore a healthy warehouse afterwards,
because the DAG resolves the real one and cannot be handed a private copy.

**What the gate actually shows.** Rendered markdown, captured from a real parked
run:

```
| published (wrong) | 501,622.18 |
| corrected         | 607,617.49 |
| delta             | 105,995.31 |

- break at `trd.trades_converted` in CTE `converted` (02_trades/05_convert_currency.sql)
- code version the original ran under: `b5e8748df0c1` (via run-xcom)
```

The delta is the Phase 0 figure to the cent, arrived at from the other
direction: Phase 0 measured it by seeding the bug, Phase 2 measured it by
remediating it.
