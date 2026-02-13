# Airflow Tests

Tests for Airflow DAGs and helpers. Run from the **repository root** so DAG and ETL paths resolve correctly.

## What’s in here

- **DAG parsing / import**: All DAGs in `airflow/dags/` load without import or parse errors.
- **DAG presence**: Required DAGs exist and have expected structure (e.g. `etl_pipeline`, `schema_analysis`).
- **Fixtures**: `conftest.py` sets up a safe test environment (e.g. mocked Airflow Variables) so DAG files can be imported without a real Airflow DB.

These tests do **not** run task logic against real databases; they validate that DAGs are loadable and structurally sound.

## Running tests

From the **repo root** (so `airflow/dags` and `etl_pipeline` are on the path as in deployment):

```bash
# From repo root
pytest airflow/tests/ -v

# With coverage (if configured)
pytest airflow/tests/ -v --cov=airflow
```

Ensure dependencies are installed (e.g. `pip install apache-airflow pytest` or use the project’s dev extras). The tests mock Airflow’s Variable/DB so you don’t need a running Airflow or metadata DB.

## Adding tests

- **New DAG**: Add an assertion in `test_dags.py` that the DAG is present and (if needed) has expected task IDs or no import errors.
- **Task-level logic**: Prefer unit tests in `etl_pipeline/tests/` for ETL code; keep this dir for DAG-level and import checks.
