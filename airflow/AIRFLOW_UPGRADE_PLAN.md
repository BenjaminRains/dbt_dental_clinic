# Airflow Upgrade Plan: 2.11.1 → 3.1.7

**Status:** Code cutover on `feat/airflow-3.1.7` (2026-07-15); clinic nightly validation still pending  
**Last updated:** 2026-07-15  
**Driver:** Align repo with laptop `.venv-airflow` on Airflow 3.x (Task SDK / api-server)  
**Target:** **Apache Airflow 3.1.7** (Python 3.11)

Related: [`DEPLOYMENT_STRATEGY.md`](DEPLOYMENT_STRATEGY.md), [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md)

---

## Why upgrade

| Reason | Detail |
|--------|--------|
| Platform line | 2.11.x is the last 2.x security line; 3.1.x is current stable major |
| Architecture | Task Execution API, FastAPI api-server UI, separate dag-processor |
| Operators | Core operators live in `apache-airflow-providers-standard` |
| Removed in 3 | `SequentialExecutor`, `schedule_interval`, `execution_date` context key, Flask `webserver` CLI |

---

## Version targets

| Component | Previous | Target |
|-----------|----------|--------|
| `apache-airflow` | 2.11.1 | **3.1.7** |
| Constraints | `constraints-2.11.1` | `constraints-3.1.7` |
| Executor | SequentialExecutor | **LocalExecutor** (SQLite OK for local) |
| UI process | `airflow webserver` | **`airflow api-server`** |
| DAG parse | embedded in scheduler | **`airflow dag-processor`** |
| Auth | FAB (bundled) | FAB via `apache-airflow-providers-fab` |
| Operators | `airflow.operators.*` | `airflow.providers.standard.operators.*` |

---

## DAG compatibility (applied on current main DAGs)

| Change | Our fix |
|--------|---------|
| `schedule_interval=` removed | `schedule=` |
| `context['execution_date']` removed | `context['logical_date']` |
| `PythonOperator` / `BashOperator` moved | `airflow.providers.standard.operators.*` |
| `Variable` / `DAG` / `TaskGroup` | `airflow.sdk` imports |
| `Variable.get(..., default_var=)` | `default=` |
| Layer 0 replica checks | **kept** from main |
| `publish_ensure_tunnel` → `--ensure-tunnel` | **kept** from main |

---

## Native start (Windows)

```powershell
mdc airflow start --scheduler
mdc airflow start --dag-processor
mdc airflow start --api-server
```

`--webserver` is an alias for `--api-server`.

---

## Validation checklist

| # | Test | Pass criteria |
|---|------|----------------|
| 1 | `pytest airflow/tests/ -v` | DAG import + structure green |
| 2 | `airflow dags list` | `etl_pipeline`, `schema_analysis`, no import errors |
| 3 | UI at :8080 | Both DAGs visible (paused) |
| 4 | Manual trigger after 9 PM Central (`etl_environment=test`) | guard → schema → validation → ETL → Layer 0 → dbt |
| 5 | Clinic cutover (later) | `etl_environment=clinic` + publish tunnel |

---

## Rollback

1. Stop scheduler / dag-processor / api-server.
2. Restore prior `.venv-airflow` backup (if kept) + metadata DB copy from before `db migrate`.
3. Checkout prior commit that pinned 2.11.1.

Note: metadata migrated for 3.x may not run cleanly under 2.11.1 without a DB restore.
