# dbt fixture

A deliberately small dbt project that reproduces a compressed version of BUG A:
a seed with a missing FX rate, a model that left-joins to it, and a downstream
model that aggregates the result away.

**This is not the demo pipeline.** The demo is the raw SQL under
`include/sql/`. This fixture exists for one reason: to prove that
`branchlin.sources.dbt` is viable, by producing a real `target/manifest.json`
and real `target/compiled/` output for the parser to consume in Phase 1.

Run it:

    python3 -m venv .venv-dbt
    .venv-dbt/bin/pip install -r requirements-dbt.txt
    cd include/dbt_fixture
    ../../.venv-dbt/bin/dbt deps --no-version-check   # (no packages; a no-op)
    ../../.venv-dbt/bin/dbt seed
    ../../.venv-dbt/bin/dbt compile
