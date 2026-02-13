"""
Pytest fixtures for Airflow DAG tests.

Provides a safe environment so DAG files can be imported without a real Airflow
metadata DB or Variables. DAGs use Variable.get() at parse time; we patch that
to return safe defaults.
"""

import os
import sys
from pathlib import Path

import pytest


# Repo root (parent of airflow/)
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
AIRFLOW_DAGS_DIR = REPO_ROOT / "airflow" / "dags"


@pytest.fixture(scope="session", autouse=True)
def airflow_test_env():
    """
    Set up minimal env so DAG parsing works without real Airflow DB.
    Patches Variable.get to return safe defaults for project_root, etl_environment, dbt_target.
    """
    # Do NOT add REPO_ROOT to sys.path: this repo has a folder named "airflow/"
    # which would shadow the real apache-airflow package when DAGs do "from airflow.models import ...".

    # Mock Airflow Variable.get so DAGs parse without DB
    try:
        from unittest.mock import patch
        with patch.dict(os.environ, {"AIRFLOW_HOME": str(REPO_ROOT / "airflow")}):
            # Patch at module load time for DagBag
            from airflow.models import Variable

            def _mock_get(key, default_var=None):
                defaults = {
                    "project_root": "/opt/airflow/dbt_dental_clinic",
                    "etl_environment": "test",
                    "dbt_target": "local",
                }
                return defaults.get(key, default_var)

            with patch.object(Variable, "get", side_effect=_mock_get):
                yield
    except ImportError:
        # Airflow not installed (e.g. CI without airflow); skip Variable patch
        yield


@pytest.fixture
def dag_bag(airflow_test_env):
    """Load all DAGs from airflow/dags; use after patching Variable."""
    pytest.importorskip("airflow")
    # Ensure repo root is not on path so "airflow" resolves to apache-airflow, not this repo's airflow/
    path_save = list(sys.path)
    try:
        if str(REPO_ROOT) in sys.path:
            sys.path.remove(str(REPO_ROOT))
        from airflow.models import DagBag
        bag = DagBag(dag_folder=str(AIRFLOW_DAGS_DIR), include_examples=False)
        return bag
    finally:
        sys.path[:] = path_save
