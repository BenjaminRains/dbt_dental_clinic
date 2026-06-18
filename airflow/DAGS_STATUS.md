# Airflow DAGs Status

Quick reference for the status of all Airflow DAGs in the OpenDental data pipeline.

**Roadmap (deployment, gaps, open decisions):** [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md)

## Implementation Status

| DAG / component | Status | Schedule | Purpose |
|-----------------|--------|----------|---------|
| **schema_analysis** | ✅ Complete | Weekly (Sun 2 AM Central) | Analyze schema, generate `tables.yml` |
| **etl_pipeline** | ✅ Complete | Nightly 9 PM Central | Extract & load data (incremental by default) |
| **dbt_build** (task group in `etl_pipeline`) | ✅ Complete | Same run as ETL (after success) | `dbt deps` + `dbt build` |
| **Production deployment** | ❌ Not started | — | Docker on host with clinic network access |
| **End-to-end validation** | ❓ Unknown | — | Manual ETL/dbt still in daily use |

**Note:** dbt is **not** a separate DAG. It runs inside `etl_pipeline` via the `dbt_build` task group when `pipeline_success` is True.

## DAG Details

### ✅ Schema Analysis DAG (`schema_analysis_dag.py`)

**Status:** Code complete — needs deployed Airflow instance

**What it does:**
- Analyzes OpenDental MySQL schema
- Generates `etl_pipeline/config/tables.yml`
- Detects breaking changes
- Sends notifications with severity levels

**When to run:**
- Scheduled: Weekly on Sunday at 2 AM Central
- Manual: Before OpenDental upgrades, when schema changes suspected, or when ETL fails with schema errors

**Outputs:**
- `etl_pipeline/config/tables.yml`
- Backup files in `logs/schema_analysis/backups/`
- Change reports in `logs/schema_analysis/reports/`

### ✅ ETL Pipeline DAG (`etl_pipeline_dag.py`)

**Status:** Code complete — needs deployed Airflow instance + env wiring

**What it does:**
- Business-hours guard (blocks 6 AM–8:59 PM Central)
- Validates configuration and connections
- Extracts from OpenDental → replication MySQL → PostgreSQL analytics
- Processes tables by performance category (large parallel, others sequential)
- Reports and notifies (even on partial failure)
- Runs dbt when ETL succeeded (ShortCircuit otherwise)

**When to run:**
- Scheduled: Nightly 9 PM Central (Mon–Sun)
- Manual: On-demand refresh (outside business hours)

**Parameters** (when triggering manually):
```python
{
    "force_full_refresh": false,
    "max_workers": 5,
    "skip_validation": false
}
```

### ✅ dbt (task group inside `etl_pipeline`)

**Status:** Code complete — integrated, not a standalone DAG

**Tasks:** `should_run_dbt` → `dbt_deps` → `dbt_build`

**Target:** Airflow Variable `dbt_target` (`local` for Compose dev, `clinic` for RDS)

**Future enhancements** (selectors, Cosmos, split slow marts): see [`DBT_DAG_PLAN.md`](DBT_DAG_PLAN.md)

## What's blocking production

See [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md) for full detail. Summary:

1. **Orchestration host not chosen** — dedicated EC2 vs VPN dev machine vs API EC2
2. **Environment gaps** — `etl_pipeline/.env_clinic`, `MYSQL_REPLICATION_*`, RDS dbt credentials
3. **No EC2 deploy path** — no script in `deployment_credentials`; not in `docs/deployment/`
4. **No verified smoke test** — Docker Compose end-to-end not confirmed

## Quick Start (local)

```bash
# One-time setup — see ORCHESTRATION_ROADMAP.md Phase A
copy .env.template .env
copy etl_pipeline\.env_test.template etl_pipeline\.env_test

docker-compose build airflow-webserver airflow-scheduler
docker-compose --profile init run --rm airflow-init
docker-compose up -d postgres mysql airflow-webserver airflow-scheduler
```

Set Airflow Variables in UI (http://localhost:8080):

| Variable | Local dev value |
|----------|-----------------|
| `etl_environment` | `test` |
| `dbt_target` | `local` |
| `project_root` | `/opt/airflow/dbt_dental_clinic` |

Enable `schema_analysis` and `etl_pipeline`; trigger manually outside business hours.

## Manual workflow replacement

| Manual (today) | Automated (target) |
|----------------|-------------------|
| `etl-run` / `mdc etl run` | `etl_pipeline` DAG |
| `run_dbt_on_ec2.ps1 -Clinic RefreshProject` | `dbt_build` task group |
| Ad-hoc schema analysis | `schema_analysis` DAG |

Keep manual commands as break-glass for single-table reruns.

## Monitoring

```bash
airflow dags list
airflow dags list-runs -d etl_pipeline
airflow tasks logs etl_pipeline dbt_build <execution_date>
pytest airflow/tests/ -v
```

## Open decisions

| Question | Status |
|----------|--------|
| Where should the 9 PM clinic run execute? | _TBD_ |
| First validation: local/test or clinic RDS? | _TBD_ |
| Nightly full `dbt build` vs split slow marts? | _TBD_ |

Record decisions in [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md).

## Resources

- [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md) — phased plan and gaps
- [`DEPLOYMENT_STRATEGY.md`](DEPLOYMENT_STRATEGY.md) — Docker local → EC2
- [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md) — run semantics
- [`README.md`](README.md) — full DAG documentation
- [`DBT_DAG_PLAN.md`](DBT_DAG_PLAN.md) — future dbt orchestration options

---

**Last Updated:** 2026-06-17  
**DAG code:** 2 DAGs + integrated dbt task group — complete  
**Next:** Phase A local smoke test (see roadmap)
