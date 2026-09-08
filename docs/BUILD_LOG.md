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
byte-identical `raw.trades` (54,475 rows, gross 14,203,360,943.73), so the
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
  layout in the brief cannot produce a 15-relation straight line -- reference
  feeds trades at file 5 of 6, and there are only 20 files total. At
* `astro dev pytest` cannot write its cache directory (`Permission denied:
  /pytest-cache-files-*`). Harmless -- tests pass -- but noisy.
