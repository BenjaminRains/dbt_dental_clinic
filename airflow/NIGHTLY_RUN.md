# Nightly ETL + dbt Run

This document defines **what a run is**, how it works, and how to run it locally (localhost) vs on EC2.

## What a Run Is

One **nightly run** = one execution of the `etl_pipeline` DAG. It consists of:

0. **Business-hours guard** – Blocks run during client business hours  
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
   - **Config-driven**: Table list and behavior come from `etl_pipeline/config/tables.yml` (from Schema Analysis DAG or manual run).

3. **Reporting** – Always runs (even on partial failure)
   - Verify loads (tracking tables)
   - Generate execution report (success/fail counts, failed tables)
   - Send notification (Slack/email)

4. **dbt** – Only when ETL succeeded
   - Short-circuit: if `pipeline_success` is False (e.g. critical failures), dbt is skipped so we don’t transform partial data.
   - When success: `dbt deps` then `dbt build` (staging → intermediate → marts).

## Failure Handling

- **Validation**: Fails fast; no ETL if config or connections are bad.
- **ETL**: Runs by category; per-table retries; non-large failures don’t stop the run; report/notify still run (trigger rule `ALL_DONE`).
- **dbt**: Runs only when `pipeline_success` is True (ShortCircuit after report).

## Schedule

- **Cron**: `0 21 * * *` (9 PM every day, Mon–Sun).
- **Timezone**: Set `AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago` so 9 PM is Central. (Done in `docker-compose.yml` for local.)

## How It Runs with Airflow

1. **Scheduler** wakes at 9 PM Central and starts one DAG run.
2. **Tasks** run in order: validation → ETL (large → medium → small → tiny) → verify → report → notify; then, if success, should_run_dbt → dbt_deps → dbt_build.
3. **Paths**: ETL and dbt use `project_root` (Airflow Variable, default `/opt/airflow/dbt_dental_clinic`). So `tables.yml` is at `{project_root}/etl_pipeline/etl_pipeline/config/tables.yml`, dbt project at `{project_root}/dbt_dental_models`.

## Local (localhost) – Docker Compose

1. **Project and DBs**
   - `docker-compose.yml` mounts `etl_pipeline` and `dbt_dental_models` under `/opt/airflow/dbt_dental_clinic/`.
   - Same `postgres` service is used for Airflow metadata and for ETL/dbt analytics; set `POSTGRES_DATABASE` (e.g. `opendental_analytics`) and use it for both.
   - Set `POSTGRES_ANALYTICS_*` in `.env` or leave defaults (they fall back to `POSTGRES_*` and `postgres` host).

2. **Config**
   - Ensure `etl_pipeline/etl_pipeline/config/tables.yml` exists (run Schema Analysis DAG or copy from repo).
   - Airflow Variables (optional): `etl_environment` = `test` or `clinic`, `project_root` = `/opt/airflow/dbt_dental_clinic`, `dbt_target` = `local`.

3. **Run**
   - Start: `docker-compose up -d airflow-webserver airflow-scheduler` (and `postgres`, `mysql` if used as source/replication).
   - First time: `docker-compose run --rm airflow-init` (with `AIRFLOW_ADMIN_*` set).
   - DAG runs nightly at 9 PM Central, or trigger manually from the UI.

4. **Test incremental**
   - Trigger the DAG with default params (`force_full_refresh: false`). Check logs for “incremental” and per-table strategy. For a full refresh test, trigger with `force_full_refresh: true`.

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
| **Run**      | One full ETL pass (incremental by default) + dbt only on success |
| **Config**   | `tables.yml` (Schema Analysis or manual) |
| **Paths**    | Variable `project_root` (default `/opt/airflow/dbt_dental_clinic`) |
| **dbt target** | Variable `dbt_target` (`local` / `clinic`) |
