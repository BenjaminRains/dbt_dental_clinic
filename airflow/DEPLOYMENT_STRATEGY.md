# Airflow Deployment Strategy

How we run Apache Airflow for the OpenDental ETL pipeline.

**Start here for status and phased plan:** [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md)

**Last updated:** 2026-06-19

---

## Production path: native Airflow (Option A)

**Decided 2026-06-19:** Clinic nightly orchestration runs **on the host** (Python venv), not in Docker.

| Why native | Detail |
|------------|--------|
| Same env as `mdc` | ETL reads `etl_pipeline/.env_clinic`; dbt/publish use the same paths and tunnels as manual runs |
| No `localhost` mismatch | Containers see their own loopback; clinic config points at laptop Postgres, SSM tunnel `:5433`, etc. |
| VPN | WireGuard on the laptop reaches OpenDental (`192.168.2.x`) — same as today's manual workflow |

**What runs:**

```bash
# One-time / per-machine setup
python -m venv .venv-airflow
.venv-airflow\Scripts\activate          # Windows
pip install -r requirements-airflow.txt
airflow db init                         # metadata DB (local Postgres, database `airflow`)

# Each session
airflow webserver & airflow scheduler   # or airflow standalone for first test
```

Set Airflow Variables (`project_root`, `etl_environment=clinic`, `dbt_target=local`, `publish_environment=clinic`). See [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md) and [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md) § Environment contract.

**Dependencies:** `requirements-airflow.txt` (same packages as `Dockerfile.airflow`). No image required for Option A.

---

## Optional: Docker Compose sandbox

Docker is **not** the clinic production path. It remains useful for **isolated DAG experiments** against `etl_environment=test`.

| Role | What Docker does |
|------|-------------------|
| **Reproducible lab** | Airflow webserver + scheduler + optional Compose postgres/mysql in one `docker-compose up` |
| **Dependency isolation** | Image from `Dockerfile.airflow` + `requirements-airflow.txt` — same deps as native venv |
| **DAG iteration** | Mount `./airflow/dags`, `./etl_pipeline`, `./dbt_dental_models` without touching clinic env |

Docker does **not** run OpenDental or clinic RDS — only the orchestrator (and optional local test DBs).

**When to use:** Phase A smoke test with `etl_pipeline/.env_test`; trying DAG changes without a native venv. **When not to use:** nightly clinic runs with `.env_clinic` as-is.

```bash
docker-compose build airflow-webserver airflow-scheduler
docker-compose --profile init run --rm airflow-init
docker-compose up -d postgres mysql airflow-webserver airflow-scheduler
```

Root `/.env` supplies Airflow metadata DB credentials and Fernet key for Compose only. ETL/dbt vars are **not** injected into Airflow containers. See [`docs/deployment/ENVIRONMENT_FILES.md`](../docs/deployment/ENVIRONMENT_FILES.md) §4.4.

Set Variables: `etl_environment=test`, `dbt_target=local`, `project_root=/opt/airflow/dbt_dental_clinic`.

---

## Environment wiring

The ETL pipeline reads **`etl_pipeline/.env_<stage>`** (`FileConfigProvider` / `settings_v2.py`). OS env vars override file values when both are set.

| Requirement | Native Option A (clinic) | Docker sandbox (test) |
|-------------|--------------------------|------------------------|
| Stage env file | `etl_pipeline/.env_clinic` on disk | `etl_pipeline/.env_test` on mounted volume |
| `OPENDENTAL_*` | Clinic MySQL via WireGuard VPN | Staging MySQL or Compose `mysql` |
| `MYSQL_REPLICATION_*` | `localhost` on laptop | Compose `mysql` or stage file |
| Analytics Postgres | Local Postgres on laptop; publish via SSM tunnel `:5433` | Compose `postgres` or stage file |
| Airflow Variables | `etl_environment`, `dbt_target`, `project_root`, `publish_environment` | Same pattern; `test` / `local` |
| Root `/.env` | **Not used** for clinic ETL | Compose substitution only |

See [`docs/deployment/ENVIRONMENT_FILES.md`](../docs/deployment/ENVIRONMENT_FILES.md) §4.4.

---

## Future paths (deferred)

| Environment | Status | Notes |
|-------------|--------|-------|
| **Dedicated EC2 orchestrator** | Deferred | Same DAGs; would need site-to-site VPN or on-prem host for OpenDental |
| **Docker on EC2** | Deferred | Same image as sandbox; only if env/`localhost` issues are solved for that host |
| **MWAA / Astronomer / k8s** | Future | Keep DAGs and Variables; change where workers run |

Record host decisions in [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md).

---

## What gets deployed (native Option A)

1. **Python venv** with `requirements-airflow.txt`
2. **DAGs** in `airflow/dags/` (scheduler reads from disk; no mount needed)
3. **Project code** at `project_root` — `{project_root}/etl_pipeline`, `{project_root}/dbt_dental_models`
4. **Stage env** — `etl_pipeline/.env_clinic` (same file `mdc` uses)
5. **Airflow metadata** — local Postgres database `airflow` (or SQLite for experiments only)
6. **Logs** — `airflow/logs/` on disk

---

## Recommended order

1. **Phase A** — Native venv + `etl_environment=test`; trigger `etl_pipeline` manually outside business hours
2. **Phase B** — Native venv + VPN + `etl_environment=clinic`; full nightly path including publish
3. **Phase C** — Enable `0 21 * * *` schedule; monitor 2–3 nights
4. **Optional anytime** — Docker sandbox for DAG dev without clinic env
5. **Later** — Re-evaluate EC2 or managed Airflow if laptop schedule is unreliable

For nightly run semantics, see [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md). For DAG reference, see [`README.md`](README.md) and [`DAGS_STATUS.md`](DAGS_STATUS.md).
