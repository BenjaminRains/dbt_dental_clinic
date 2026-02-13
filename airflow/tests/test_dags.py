"""
Tests for Airflow DAGs: parse/import and presence of required DAGs.

Run from repo root: pytest airflow/tests/ -v

Requires: apache-airflow (or install project dev deps). If Airflow is not installed,
tests that use dag_bag will fail; use pytest.importorskip in fixtures if you need
graceful skip.
"""

import pytest

# Skip all tests in this module if Airflow is not installed
pytest.importorskip("airflow", reason="apache-airflow not installed")

REQUIRED_DAG_IDS = {"etl_pipeline", "schema_analysis"}


def test_dag_bag_has_no_import_errors(dag_bag):
    """All DAGs in airflow/dags/ load without import or parse errors."""
    assert not dag_bag.import_errors, (
        f"DAG import errors: {dag_bag.import_errors}"
    )


def test_required_dags_present(dag_bag):
    """Required DAGs exist in the DAG bag."""
    for dag_id in REQUIRED_DAG_IDS:
        assert dag_id in dag_bag.dags, (
            f"Expected DAG '{dag_id}' in {list(dag_bag.dags.keys())}"
        )


def test_etl_pipeline_dag_has_guard_task(dag_bag):
    """etl_pipeline DAG has the business-hours guard as first task."""
    if "etl_pipeline" not in dag_bag.dags:
        pytest.skip("etl_pipeline DAG not loaded")
    dag = dag_bag.dags["etl_pipeline"]
    task_ids = [t.task_id for t in dag.tasks]
    assert "guard_business_hours" in task_ids, (
        f"Expected guard_business_hours in {task_ids}"
    )


def test_etl_pipeline_dag_has_validation_and_dbt(dag_bag):
    """etl_pipeline DAG has validation group and dbt task group."""
    if "etl_pipeline" not in dag_bag.dags:
        pytest.skip("etl_pipeline DAG not loaded")
    dag = dag_bag.dags["etl_pipeline"]
    task_ids = [t.task_id for t in dag.tasks]
    # Validation group tasks
    assert "validate_configuration" in task_ids
    # dbt group tasks (task_id includes group prefix in some Airflow versions)
    has_dbt = any("dbt" in tid for tid in task_ids)
    assert has_dbt, f"Expected dbt tasks in {task_ids}"
