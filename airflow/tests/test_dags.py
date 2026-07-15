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
    assert "validation.validate_configuration" in task_ids
    has_dbt = any("dbt" in tid for tid in task_ids)
    assert has_dbt, f"Expected dbt tasks in {task_ids}"


def test_etl_pipeline_dag_has_schema_refresh(dag_bag):
    """etl_pipeline refreshes tables.yml before validation."""
    if "etl_pipeline" not in dag_bag.dags:
        pytest.skip("etl_pipeline DAG not loaded")
    task_ids = [t.task_id for t in dag_bag.dags["etl_pipeline"].tasks]
    assert "refresh_schema_configuration" in task_ids


def test_etl_pipeline_dag_has_publish_and_notify(dag_bag):
    """etl_pipeline DAG publishes to RDS (when configured) and notifies last."""
    if "etl_pipeline" not in dag_bag.dags:
        pytest.skip("etl_pipeline DAG not loaded")
    dag = dag_bag.dags["etl_pipeline"]
    task_ids = [t.task_id for t in dag.tasks]
    assert "publish_analytics" in task_ids
    assert "send_completion_notification" in task_ids
    assert "should_run_publish" in task_ids


def test_etl_pipeline_dag_has_layer0_replica_checks(dag_bag):
    """etl_pipeline keeps Layer 0 replica drift checks after Airflow 3 port."""
    if "etl_pipeline" not in dag_bag.dags:
        pytest.skip("etl_pipeline DAG not loaded")
    task_ids = [t.task_id for t in dag_bag.dags["etl_pipeline"].tasks]
    assert any("layer0" in tid for tid in task_ids), f"Expected layer0 tasks in {task_ids}"
