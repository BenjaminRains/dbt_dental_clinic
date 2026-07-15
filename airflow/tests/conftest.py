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
_STUBS_DIR = REPO_ROOT / "scripts" / "airflow" / "windows_posix_stubs"

# Windows: apply POSIX shims before DagBag (needs SIGALRM/setitimer stubs)
if sys.platform == "win32" and str(_STUBS_DIR) not in sys.path:
    sys.path.insert(0, str(_STUBS_DIR))
    try:
        import airflow_win_patch  # noqa: F401
    except ImportError:
        pass


@pytest.fixture(scope="session", autouse=True)
def airflow_test_env():
    """
    Set up minimal env so DAG parsing works without real Airflow DB.
    Patches Variable.get to return safe defaults for project_root, etl_environment, dbt_target.
    """
    # Do NOT add REPO_ROOT to sys.path: this repo has a folder named "airflow/"
    # which would shadow the real apache-airflow package when DAGs do "from airflow...".

    try:
        from unittest.mock import patch

        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOME": str(REPO_ROOT / "airflow"),
                # Avoid DagBag SIGALRM import timeout path on Windows
                "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT": "0",
            },
        ):
            # Airflow 3 Task SDK Variable (DAGs import from airflow.sdk)
            from airflow.sdk import Variable

            def _mock_get(key, default=None, **kwargs):
                defaults = {
                    "project_root": str(REPO_ROOT),
                    "etl_environment": "test",
                    "dbt_target": "local",
                    "publish_environment": "",
                }
                return defaults.get(key, default)

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
        # lib.mdc_runner resolves via dags folder on PYTHONPATH
        dags_path = str(AIRFLOW_DAGS_DIR)
        if dags_path not in sys.path:
            sys.path.insert(0, dags_path)
        from airflow.models import DagBag

        bag = DagBag(dag_folder=str(AIRFLOW_DAGS_DIR), include_examples=False)
        return bag
    finally:
        sys.path[:] = path_save
