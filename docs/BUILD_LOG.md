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
