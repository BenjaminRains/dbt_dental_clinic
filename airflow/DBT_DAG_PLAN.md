### High-Level Plan: Airflow DAG for dbt Project

This document outlines options and a recommended starting approach for orchestrating the `dbt_dental_models` project with Airflow. We'll implement after confirming the open questions below.

### What “creating a DAG” can mean for dbt here
- **Minimal**: One task that runs the entire dbt project (e.g., `dbt build`).
- **Layered**: Separate tasks for stages using selectors (e.g., staging/intermediate/marts).
- **Model-level**: One Airflow task per dbt node with dependencies from the manifest. Best done via Astronomer Cosmos.

### Operator choices
- **BashOperator**: Simple; runs dbt CLI directly on the Airflow worker.
- **DockerOperator/KubernetesPodOperator**: Run dbt in an isolated container when workers don’t have dbt.
- **Astronomer Cosmos**: Auto-generates Airflow tasks from the dbt manifest for model-level DAGs.

### Suggested starting approach (simple, fast to value)
- Start with a single-DAG, single-task `BashOperator` that runs, in sequence:
  - `dbt deps`
  - `dbt build --project-dir /opt/airflow/dbt_dental_models --profiles-dir /opt/airflow/.dbt --target prod`
- Schedule nightly; add retries. Mount the repo into Airflow so dbt can run against `dbt_dental_models`.

### Iteration ideas (future)
- Add a `freshness` task before `build`.
- Split into staged `BashOperator` tasks using selectors from `selectors.yml`:
  - **Option 1 - Layer-based**: `staging_only` → `intermediate_only` → `marts_only`
  - **Option 2 - Domain-based**: `insurance_workflow`, `clinical_workflow`, `financial_workflow` (parallel)
  - **Option 3 - System-based**: Run your documented system workflows independently
  - **Option 4 - Hybrid**: `daily_refresh` (morning) + `reference_data` (weekly)
- Adopt Astronomer Cosmos for model-level tasks and dependency visualization.
- Persist artifacts/logs to a durable volume or cloud storage (S3/GCS) and wire into observability.
- Use state selection (e.g., `--select state:modified+`) to speed runs.

### Selector Examples (see selectors.yml)

#### Daily Operations
```bash
# Daily critical models (insurance, AR, appointments)
dbt run --selector daily_critical

# Only incremental models for fast refresh
dbt run --selector incremental_only
```

#### Domain-Specific Workflows
```bash
# Run insurance workflow independently
dbt run --selector insurance_workflow

# Run clinical workflow
dbt run --selector clinical_workflow

# Financial reporting models
dbt run --selector financial_workflow
```

#### Layer-Based Orchestration (Sequential)
```bash
# Step 1: Staging layer
dbt run --selector staging_only

# Step 2: Intermediate layer
dbt run --selector intermediate_only

# Step 3: Marts layer
dbt run --selector marts_only
```

#### System-Based Workflows
```bash
# System A: Fee Processing
dbt run --selector system_a_fee_processing

# System B: Insurance Management
dbt run --selector system_b_insurance

# System C: Appointments
dbt run --selector system_c_appointments
```

#### CI/CD & Testing
```bash
# Smoke test for deployments
dbt run --selector smoke_test

# Production-critical only
dbt run --selector production_critical
```

### Environment and paths
- Mount `dbt_dental_models` under `/opt/airflow/dbt_dental_models` in the Airflow runtime.
- Place `profiles.yml` under `/opt/airflow/.dbt/profiles.yml` (or confirm existing path) and set the appropriate `target` (local/demo/clinic).
- Ensure dbt is installed in the Airflow environment, or run via containerized operator.

### Questions to confirm before implementation
1. Are we running Airflow via this repo’s `docker-compose` (using `Dockerfile.airflow`) or another environment (e.g., MWAA/Astronomer/k8s)?
2. Do you want the initial DAG to be a single `dbt build` task, or split into staged tasks right away?
3. Confirm `profiles.yml` path and `target` to use. Is `dbt_dental_models/profiles.yml` sufficient, or should we mount `/opt/airflow/.dbt/profiles.yml`?
4. Desired schedule and retry policy (e.g., daily 02:00, 2 retries, 5 minutes delay).
5. Where should we persist dbt `target/` artifacts and logs (local volume vs S3/GCS)?
6. Should we include `dbt source freshness` and `dbt test` explicitly, or rely on `dbt build`?
7. Any immediate need for model-level observability/retries per node (i.e., adopt Cosmos now vs later)?

### Next steps
- After confirming the above, scaffold an initial DAG in `airflow/dags/` and update the runtime mounts/env as needed.


