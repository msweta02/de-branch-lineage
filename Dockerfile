FROM astrocrpublic.azurecr.io/runtime:3.3-7

# Put `include/` on the import path so `branchlin` is importable by name.
#
# Airflow already has /usr/local/airflow on sys.path, which is why the DAG files
# can do `from include.runner import ...`. That is not enough for the library:
# branchlin's own modules import each other absolutely (`from branchlin.parser
# import ...`), so importing it as `include.branchlin` would fail on the first
# internal import. The package has to be reachable as `branchlin`.
#
# Set here rather than in `.env` because `.env` is gitignored -- a setting the
# pipeline depends on has to travel with the repository. Without it the DAG
# tasks fail to record their code version with
# `ModuleNotFoundError: No module named 'branchlin'`, and the DIAGNOSE DAG
# cannot import the tracer at all.
ENV PYTHONPATH=/usr/local/airflow:/usr/local/airflow/include
