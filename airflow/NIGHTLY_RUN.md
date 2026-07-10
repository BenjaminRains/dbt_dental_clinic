# Nightly ETL + dbt Run

This document defines **what a run is**, how it works, and how to run it on the **Option A laptop** (native Airflow). Docker Compose is an optional sandbox only — see [`DEPLOYMENT_STRATEGY.md`](DEPLOYMENT_STRATEGY.md).

**Deployment roadmap and open decisions:** [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md)

## What a Run Is

One **nightly run** = one execution of the `etl_pipeline` DAG. It consists of:

0. **Schema refresh** – Regenerate `tables.yml` from live OpenDental (`analyze_opendental_schema.py`) so ETL does not fail on schema drift.
1. **Business-hours guard** – Blocks run during client business hours  
   - **Pipeline MUST NOT run during 6 AM–8:59 PM Central** because ETL hogs client server MySQL.  
   - First task checks current wall clock (America/Chicago). If in that window, the run **fails** (no ETL runs).  
   - Allowed window: **9 PM–5:59 AM Central only** (applies to scheduled and manual triggers).

1. **Validation** – Config and connections
   - `tables.yml` exists and is not too old (< 90 days)
   - Connections to source (OpenDental MySQL), replication MySQL, and analytics PostgreSQL succeed
   - Optional schema drift check

2. **ETL (full pass, incremental load)** – Config-driven
   - **Incremental by default**: `force_full_refresh=False`. Each table uses strategy from `tables.yml` (`extraction_strategy`, `incremental_columns`, `primary_incremental_column`, etc.). First load or stale data can trigger full refresh per table (loader logic).
   - **Full pass**: All tables in order by size: large → medium → small → tiny. One run = one complete pass over the config.
   - **Config-driven**: Table list and behavior come from `etl_pipeline/config/tables.yml`, regenerated at the start of each run (`refresh_schema_configuration`).

3. **Reporting** – Always runs (even on partial failure)
   - Verify loads (tracking tables)
   - Generate execution report (success/fail counts, failed tables)
   - Send notification (Slack/email)

4. **dbt** – Only when ETL succeeded
   - Short-circuit: if `pipeline_success` is False, dbt is skipped.
   - When success: `mdc dbt invoke --env {dbt_target}` → deps + build (default `local`).
   - **Not yet wired:** `dbt source freshness` (late-data checks on `raw._loaded_at` are defined in source YAML but not executed nightly — see [`DBT_DAG_PLAN.md`](DBT_DAG_PLAN.md) § Wire dbt source freshness).

5. **Publish** – Only when ETL succeeded and `publish_environment` is set (e.g. `clinic`)
   - `mdc publish analytics --env clinic` via SSM tunnel on **127.0.0.1:5433**
   - **You start the tunnel manually** before the run: `mdc tunnel clinic-db` (separate terminal; wait for port 5433).

6. **Notification** – After report and publish (when configured).

## Failure Handling

- **Validation**: Fails fast; no ETL if config or connections are bad.
- **ETL**: Runs by category; per-table retries; non-large failures don’t stop the run; report/notify still run (trigger rule `ALL_DONE`).
- **dbt**: Runs only when `pipeline_success` is True (ShortCircuit after report).
- **Publish**: Runs only when `publish_environment` is set and ETL succeeded; requires **manual** `mdc tunnel clinic-db` (port 5433) before the run.

## Schedule

- **Cron**: `0 21 * * *` (9 PM every day, Mon–Sun).
- **Timezone**: Set `AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago` so 9 PM is Central (env var or `airflow.cfg` on native; also set in `docker-compose.yml` for sandbox).

## How It Runs with Airflow

1. **Scheduler** wakes at 9 PM Central and starts one DAG run.
2. **Tasks** run in order: guard → schema refresh → validation → ETL → verify → report → (if success) dbt → publish → notify.
3. **Paths**: Variable `project_root` = repo root on disk (native Option A).

## Option A — Native Airflow on laptop

1. **Preflight (before 9 PM or manual trigger)**
   - WireGuard connected (OpenDental).
   - Terminal 1: `mdc tunnel clinic-db` — wait for **Port 5433 opened**.
   - Airflow scheduler + webserver running (Python venv, not Docker).

2. **Airflow Variables**

   | Variable | Clinic nightly |
   |----------|----------------|
   | `project_root` | Repo root path |
   | `etl_environment` | `clinic` |
   | `dbt_target` | `local` |
   | `publish_environment` | `clinic` |

3. **Run** — Scheduled at 9 PM Central or trigger manually from the UI (outside business hours).

See [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md) and [`docs/deployment/CLINIC_ANALYTICS_WORKFLOW.md`](../docs/deployment/CLINIC_ANALYTICS_WORKFLOW.md).

## Local (localhost) – Docker Compose (optional sandbox only)

Not used for Option A clinic nightly runs. See `docker-compose.yml` comments.

## EC2

1. **Paths**
   - Set Airflow Variable `project_root` to the path where the repo is deployed (e.g. `/opt/dbt_dental_clinic`). Same layout: `{project_root}/etl_pipeline`, `{project_root}/dbt_dental_models`.

2. **Env**
   - Source/replication/analytics credentials and `POSTGRES_ANALYTICS_*` must be in the environment of the Airflow worker (systemd, Docker, or MWAA env vars).
   - Set `dbt_target` = `clinic` (or whatever profile you use on EC2).

3. **Timezone**
   - If Airflow runs on EC2 without Docker, set `AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago` in the Airflow config or environment so the 9 PM schedule is Central.

## Summary

| Item        | Value |
|------------|--------|
| **Schedule** | 9 PM Central, Mon–Sun |
| **Run**      | Schema refresh + incremental ETL + dbt (on success) + publish (when configured) |
| **Config**   | `tables.yml` regenerated each run (`refresh_schema_configuration`) |
| **Paths**    | Variable `project_root` (repo root on disk) |
| **dbt target** | Variable `dbt_target` (`local` for laptop build + publish) |

### Observed run times (clinic, Option A)

| Phase | Duration |
|-------|----------|
| Schema refresh | ~7 min (446 tables; logs Mar–Jun 2026) |
| Incremental ETL | ~25 min (every couple of days) |
| Full `dbt build` | ~52 min (if included; dominated by `mart_patient_retention`) |
| Publish to RDS | varies |

**Typical pre-dbt window:** ~32 min (schema + ETL). With full dbt + publish, plan for **~1.5–2 hours** from 9 PM.

---

## Environment prerequisites

Before a run can succeed, the Airflow worker must resolve **all three ETL connections** (source MySQL, replication MySQL, analytics PostgreSQL) and **dbt analytics credentials**.

| Source | Used by | Notes |
|--------|---------|-------|
| `etl_pipeline/.env_<stage>` | ETL (`get_settings`) | Stage from Airflow Variable `etl_environment` (`test`, `clinic`, …) |
| OS env vars | ETL + dbt | Override file values when set (`settings_v2` precedence) |
| Root `/.env` | Docker Compose sandbox only | **Not used** for Option A clinic nightly |
| `dbt_dental_models/.env_local` | dbt on laptop | Via `mdc dbt invoke --env local` |

**Option A clinic:** `etl_pipeline/.env_clinic` on disk; native Airflow; WireGuard + `mdc tunnel clinic-db` for publish. See [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md).

**Docker sandbox (optional):** `etl_environment=test`, `dbt_target=local`, `etl_pipeline/.env_test` on mounted volume.

---

**Last updated:** 2026-06-19
