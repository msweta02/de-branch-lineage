"""Airflow DAG for the 01_reference SQL module.

Tasks are the .sql files in include/sql/01_reference, one task per file, executed in
filename order. The factory in include/pipeline_dag.py builds the DAG and wires
this module to the others with Assets.

Note for anyone editing this file: Airflow's safe-mode DAG discovery only parses
files containing both "dag" and "airflow", so the word airflow has to appear
here even though nothing is imported from it directly.
"""

from include.pipeline_dag import build_module_dag

reference_pipeline = build_module_dag("01_reference")
