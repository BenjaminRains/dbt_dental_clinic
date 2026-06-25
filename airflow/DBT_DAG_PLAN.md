# dbt Orchestration Plan

**Status:** Initial implementation **complete** (integrated in `etl_pipeline` DAG).  
**This document:** Future enhancements only — selectors, layered runs, Cosmos.  
**Deployment roadmap:** [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md)

---

## Current implementation (done)

dbt runs inside `airflow/dags/etl_pipeline_dag.py` as the `dbt_build` task group:

1. `should_run_dbt` — ShortCircuit; skips when `pipeline_success` is False
2. `dbt_deps` — install package dependencies
3. `dbt_build` — `dbt build --target {dbt_target}` (run + test)

There is **no separate `dbt_build` DAG**. A standalone DAG was considered early in planning but superseded by same-run orchestration (see [discussion](30475cd5-d66b-42f7-84a3-3ae3466c2e9b), 2026-06-17).

**Airflow Variables:**
- `dbt_target`: `local` (laptop build, Option A) or `clinic` (RDS build on EC2)
- `project_root`: path to repo root containing `dbt_dental_models/`

**Profiles:** `dbt_dental_models/profiles.yml`; `DBT_PROFILES_DIR` set to project dir in BashOperator.

---

## Resolved decisions (initial scope)

| Question | Decision |
|----------|----------|
| Separate dbt DAG vs integrated? | **Integrated** in `etl_pipeline` (ShortCircuit on ETL success) |
| Operator | **BashOperator** — `dbt deps` then `dbt build` |
| Schedule | Same as ETL — nightly 9 PM Central |
| Initial scope | Single `dbt build` (full project) |

---

## Open decisions (still TBD)

Record answers in [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md).

| # | Question | Options |
|---|----------|---------|
| 1 | Nightly dbt scope | Full `dbt build` (~52 min; `mart_patient_retention` dominates) · Split slow marts to weekly selector |
| 2 | Model-level observability | Stay on BashOperator · Adopt Astronomer Cosmos later |
| 3 | Artifact persistence | Local volume only · S3/GCS for `target/` and logs |

---

## Future enhancement options

### Operator choices (if refactoring)

- **BashOperator** (current): Simple; runs dbt CLI on the Airflow worker.
- **DockerOperator / KubernetesPodOperator**: Isolated container when workers lack dbt.
- **Astronomer Cosmos**: Auto-generates tasks from dbt manifest for per-model retries and graph view.

### Layered runs (selectors)

Split into staged tasks using `dbt_dental_models/selectors.yml`:

```bash
# Layer-based (sequential)
dbt run --selector staging_only
dbt run --selector intermediate_only
dbt run --selector marts_only

# Domain-based (parallel potential)
dbt run --selector insurance_workflow
dbt run --selector clinical_workflow
dbt run --selector financial_workflow

# Daily critical subset
dbt run --selector daily_critical
dbt run --selector incremental_only
```

### Wire dbt source freshness (late / missing data)

**Status:** Configured in dbt, **not run** in the nightly DAG.

Source freshness thresholds are defined on OpenDental raw sources (`dbt_dental_models/models/staging/opendental/_sources/*.yml` and `models/staging/raw/_sources.yml`): `loaded_at_field: "_loaded_at"` with source-level defaults (warn 24h, error 30d) and tighter per-table overrides for transactional tables (e.g. appointment, payment — 24–48h).

**Gap:** `dbt build` does not execute source freshness. `verify-loads` (ETL reporting) checks that every configured table has `load_status=success` in `raw.etl_load_status` but does **not** fail on stale `_loaded_at` (the 36h “recently touched” count is informational only). The `check_etl_freshness` macro in `macros/utils/etl_tracking_helpers.sql` is unused.

**Proposed wiring** (in `etl_pipeline_dag.py` `dbt_build` task group, after `should_run_dbt`):

1. Add `dbt_source_freshness` — `mdc dbt invoke --env {dbt_target} -- source freshness` (fail the DAG on `error` status).
2. Keep `dbt_deps` → `dbt_build` order; run freshness after deps (packages available) and **before** `build` so stale raw data is caught before transforms.
3. Optionally align Airflow alerts with dbt freshness results (Slack/email on warn or error).

**Related:** [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md) (dbt step), [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md) Phase D, [`dbt_docs/data_quality.md`](../dbt_docs/data_quality.md) (Timeliness).

### Other iteration ideas

- Use `--select state:modified+` for faster CI/CD runs
- Run `mart_patient_retention` on a separate weekly schedule (see TODO: Optimize mart_patient_retention)
- Persist artifacts to durable storage and wire into observability

### Environment and paths

- Mount `dbt_dental_models` under `{project_root}/dbt_dental_models`
- Ensure `POSTGRES_ANALYTICS_*` env vars reach the worker for `dbt_target=clinic`
- See [`ORCHESTRATION_ROADMAP.md`](../airflow/ORCHESTRATION_ROADMAP.md) for native Option A env contract

---

## When to revisit this doc

- After Phase C production cutover (roadmap) — if nightly `dbt build` duration is unacceptable
- When adding Cosmos or selector-based task groups
- When multi-tenant schema routing (MDC/GLIC) affects dbt targets

**Last updated:** 2026-06-23
