### High-Level Plan: Airflow DAG for dbt Project

This document outlines options and a recommended starting approach for orchestrating the `dbt_dental_clinic_prod` project with Airflow. We'll implement after confirming the open questions below.

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
  - `dbt build --project-dir /opt/airflow/dbt/dbt_dental_clinic_prod --profiles-dir /opt/airflow/.dbt --target prod`
- Schedule nightly; add retries. Mount the repo into Airflow so dbt can run against `dbt_dental_clinic_prod`.

### Iteration ideas (future)
- Add a `freshness` task before `build`.
- Split into two `BashOperator` tasks with selectors (e.g., `+tag:intermediate` then `+tag:marts`, or use folder-based selectors).
- Adopt Astronomer Cosmos for model-level tasks and dependency visualization.
- Persist artifacts/logs to a durable volume or cloud storage (S3/GCS) and wire into observability.
- Use state selection (e.g., `--select state:modified+`) to speed runs.

### Environment and paths
- Mount `dbt_dental_clinic_prod` under `/opt/airflow/dbt/dbt_dental_clinic_prod` in the Airflow runtime.
- Place `profiles.yml` under `/opt/airflow/.dbt/profiles.yml` (or confirm existing path) and set the appropriate `target` (prod/dev).
- Ensure dbt is installed in the Airflow environment, or run via containerized operator.

### Questions to confirm before implementation
1. Are we running Airflow via this repo’s `docker-compose` (using `Dockerfile.airflow`) or another environment (e.g., MWAA/Astronomer/k8s)?
2. Do you want the initial DAG to be a single `dbt build` task, or split into staged tasks right away?
3. Confirm `profiles.yml` path and `target` to use. Is `dbt_dental_clinic_prod/profiles.yml` sufficient, or should we mount `/opt/airflow/.dbt/profiles.yml`?
4. Desired schedule and retry policy (e.g., daily 02:00, 2 retries, 5 minutes delay).
5. Where should we persist dbt `target/` artifacts and logs (local volume vs S3/GCS)?
6. Should we include `dbt source freshness` and `dbt test` explicitly, or rely on `dbt build`?
7. Any immediate need for model-level observability/retries per node (i.e., adopt Cosmos now vs later)?

### Next steps
- After confirming the above, scaffold an initial DAG in `airflow/dags/` and update the runtime mounts/env as needed.


