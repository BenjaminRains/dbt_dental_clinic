# Airflow Deployment Strategy

This document describes how we deploy Apache Airflow for the OpenDental ETL pipeline, **how Docker fits in**, and the options for local vs EC2 vs future managed services.

---

## How Docker Fits In

Docker is the **primary way we run Airflow** for this project.

| Role | What Docker does |
|------|-------------------|
| **Same runtime everywhere** | Webserver, scheduler, and (with LocalExecutor) task execution all run in the same image. No “works on my machine” drift between dev and deploy. |
| **Dependency isolation** | Airflow, ETL deps (PyYAML, SQLAlchemy, pymysql, psycopg2), dbt-core, and providers live in one image built from `Dockerfile.airflow` + `requirements-airflow.txt`. |
| **Reproducible builds** | A fixed base image (`apache/airflow:2.7.1`) + our requirements gives a reproducible environment for DAG runs. |
| **Deployment unit** | We deploy by building this image and running it (Compose locally, or the same image on EC2/ECS later). DAGs and config are mounted or baked in. |

Docker does **not** run the client’s MySQL or your analytics Postgres; those are external. Docker runs only Airflow (webserver + scheduler + workers when using LocalExecutor).

---

## Deployment Options (High Level)

| Environment | How Airflow runs | Use case |
|-------------|------------------|----------|
| **Local (developer machine)** | Docker Compose: `airflow-webserver`, `airflow-scheduler`, optional `airflow-init`. Postgres/MySQL in Compose or host. | Dev, testing DAGs, running nightly pipeline against local/staging DBs. |
| **EC2 (e.g. clinic)** | Same Docker image; run with Docker Compose or plain `docker run` on an EC2 instance. DAGs and project dir mounted; env from `.env` or secrets. | Production orchestration for a single clinic or shared staging. |
| **Future: MWAA / Astronomer / k8s** | Replace *where* the image runs; DAGs and variables/connections stay the same. We’d adapt the deployment tool (Terraform, Helm, etc.), not redesign the DAGs. | Multi-tenant or high-availability production. |

So: **Docker defines the Airflow runtime**; the **deployment strategy** is “run that image in the right place with the right mounts and env.”

---

## What Gets Deployed

1. **Image**
   - Built from repo root: `docker build -f Dockerfile.airflow -t airflow-dbt-dental .`
   - Uses `requirements-airflow.txt` (Airflow + ETL deps + dbt + pendulum, Slack provider).
   - Used by all Compose services that run Airflow (webserver, scheduler, init).

2. **DAGs**
   - Live in `airflow/dags/` in the repo.
   - In Docker: mounted as `./airflow/dags:/opt/airflow/dags` so changes are visible without rebuilding.

3. **Project code (ETL + dbt)**
   - ETL and dbt run *inside* the same container (LocalExecutor).
   - Mounted so we don’t bake repo into image:
     - `./etl_pipeline` → `/opt/airflow/dbt_dental_clinic/etl_pipeline`
     - `./dbt_dental_models` → `/opt/airflow/dbt_dental_clinic/dbt_dental_models`
   - Airflow Variable `project_root` defaults to `/opt/airflow/dbt_dental_clinic` (override on EC2 if path differs).

4. **Config and secrets**
   - **Metadata DB**: `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (Postgres).
   - **ETL / dbt**: Source MySQL and analytics Postgres via env (e.g. `OPENDENTAL_SOURCE_*`, `POSTGRES_ANALYTICS_*`).
   - Set in `docker-compose.yml` env section and/or `.env`; on EC2, use same env or a secrets manager.

5. **Persistence**
   - Airflow metadata: Postgres (Compose `postgres` or RDS on EC2).
   - Logs: `./airflow/logs` mounted so they survive container restarts.

---

## Local Deployment (Docker Compose)

- **Purpose**: Run Airflow on your machine for development and testing (including nightly DAG at 9 PM Central and business-hours guard).
- **How**: Use the same Docker setup as production-like runs, with local or staging DBs.

**Steps:**

1. **Env**
   - Copy `.env.template` (or equivalent) to `.env`.
   - Set at least: `POSTGRES_*`, `AIRFLOW_FERNET_KEY`, `AIRFLOW_ADMIN_*`, `OPENDENTAL_SOURCE_*` (or point to a staging MySQL). Optionally `POSTGRES_ANALYTICS_*` (defaults in Compose point to same Postgres).

2. **Build**
   ```bash
   docker-compose build airflow-webserver airflow-scheduler
   ```

3. **Init (once)**
   ```bash
   docker-compose run --rm airflow-init
   ```
   Creates Airflow metadata DB and admin user.

4. **Run**
   ```bash
   docker-compose up -d airflow-webserver airflow-scheduler
   ```
   Also ensure `postgres` (and `mysql` if used as source) are up.

5. **Use**
   - UI: http://localhost:8080  
   - DAGs load from `airflow/dags/`; ETL/dbt use mounted `etl_pipeline` and `dbt_dental_models`.  
   - Set Variables (`project_root`, `dbt_target`, `etl_environment`) in the UI if you need to override defaults.

**Docker’s role here**: One Compose file defines webserver, scheduler, init, and DBs; same image and mounts everywhere so local runs mirror deploy behavior.

---

## EC2 Deployment (Same Image, Different Host)

- **Purpose**: Run Airflow on a single EC2 instance (e.g. clinic or staging) with the same DAGs and logic as local.
- **How**: Use the same Docker image and Compose (or equivalent) on EC2; only env and mounts change.

**High level:**

1. **Build and push image** (or build on EC2).
   - From repo: `docker build -f Dockerfile.airflow -t your-registry/airflow-dbt-dental:tag .` and push, or copy Dockerfile + requirements and build on the instance.

2. **On EC2**
   - Install Docker (and Docker Compose if desired).
   - Clone/copy repo (or copy only `airflow/`, `etl_pipeline/`, `dbt_dental_models/`, `docker-compose.yml`, `.env`).
   - Set `.env` (or export) with production values: source MySQL, analytics Postgres (e.g. RDS), `AIRFLOW_FERNET_KEY`, `AIRFLOW_ADMIN_*`, `POSTGRES_ANALYTICS_*`, etc.

3. **Compose on EC2**
   - Use the same `docker-compose.yml` (or a slim variant that only runs `airflow-webserver` + `airflow-scheduler` and points to RDS for metadata).
   - Mount project dir (or baked path) so `project_root` matches, e.g. `/opt/dbt_dental_clinic`.
   - Set Airflow Variable `project_root` to that path and `dbt_target` to the correct profile (e.g. `clinic`).

4. **Security / networking**
   - Restrict UI (e.g. security group, VPN, or ALB + auth). Do not expose Airflow or DBs to the public internet without auth.

**Docker’s role here**: The same image and Compose layout as local; only the host and env change. No need for a separate “EC2 Airflow stack” — it’s the same stack in a different place.

---

## Recommended Deployment Order

1. **Local**
   - Get Compose up; run `airflow-init`; open UI; trigger DAGs manually; confirm business-hours guard and 9 PM schedule.
2. **EC2 (optional)**
   - Deploy same image + Compose (or equivalent) on EC2; wire env and `project_root`/`dbt_target`; run nightly against clinic/staging DBs.
3. **Later**
   - If you move to MWAA, Astronomer, or k8s, keep DAGs and Variables/Connections; only change where and how the image (or managed equivalent) runs.

---

## Summary

- **Docker** = standard runtime for Airflow + ETL + dbt; one image, same deps everywhere.
- **Docker Compose** = how we run that image locally and (optionally) on EC2.
- **Deployment strategy** = build the image, mount DAGs and project code, set env (and Variables/Connections), run webserver + scheduler (and init once). Same approach locally and on EC2; later you can swap the execution environment (e.g. MWAA) without redefining the DAGs.

For nightly run details (schedule, business-hours guard, incremental ETL + dbt), see `NIGHTLY_RUN.md`. For DAG status and usage, see `README.md` and `DAGS_STATUS.md`.
