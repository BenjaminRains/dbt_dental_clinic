# Airflow Upgrade Plan: → 3.2.2

**Status:** Security bump on `chore/security-dependency-bumps` (2026-07-15); clinic nightly validation still pending  
**Last updated:** 2026-07-15  
**Driver:** Clear Dependabot Critical/High advisories (JWT logout, template injection, deserialization, example_xcom); keep Task SDK / api-server line  
**Target:** **Apache Airflow 3.2.2** (Python 3.11)

Related: [`DEPLOYMENT_STRATEGY.md`](DEPLOYMENT_STRATEGY.md), [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md)

---

## Why upgrade

| Reason | Detail |
|--------|--------|
| Security | 3.2.2 patches JWT-still-valid-after-logout, template neutralization, XCom deserialization, and related Highs |
| Platform line | Prior cutover landed on 3.1.7; 3.2.2 is the security floor for those advisories |
| Architecture | Task Execution API, FastAPI api-server UI, separate dag-processor (unchanged vs 3.1.x) |
| Operators | Core operators live in `apache-airflow-providers-standard` |
| Removed in 3 | `SequentialExecutor`, `schedule_interval`, `execution_date` context key, Flask `webserver` CLI |

---

## Version targets

| Component | Previous | Target |
|-----------|----------|--------|
| `apache-airflow` | 3.1.7 | **3.2.2** |
| Constraints | `constraints-3.1.7` | `constraints-3.2.2` |
| Executor | LocalExecutor | **LocalExecutor** (SQLite OK for local) |
| UI process | `airflow api-server` | **`airflow api-server`** |
| DAG parse | `airflow dag-processor` | **`airflow dag-processor`** |
| Auth | FAB via `apache-airflow-providers-fab` | FAB (unchanged opt-in) |
| Operators | `airflow.providers.standard.operators.*` | unchanged |

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

`airflow/.env.native` must include `AIRFLOW__API_AUTH__JWT_SECRET` (Airflow 3.2+ api-server).  
`init-airflow-native.ps1` creates or appends it automatically.

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
3. Checkout prior commit that pinned 3.1.7.
