# Airflow DAGs Status

Quick reference for the status of all Airflow DAGs in the OpenDental data pipeline.

**Roadmap (deployment, gaps, open decisions):** [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md)

## Implementation Status

| DAG / component | Status | Schedule | Purpose |
|-----------------|--------|----------|---------|
| **schema_analysis** | ✅ Complete | Manual / optional | Change reports + notifications; **not** on nightly path |
| **etl_pipeline** | ✅ Complete | Nightly 9 PM Central | Schema refresh → ETL → dbt → publish |
| **dbt_build** (task group in `etl_pipeline`) | ✅ Complete | Same run as ETL (after success) | `dbt deps` + `dbt build` |
| **Production deployment** | 🔄 Option A | Dev laptop + VPN; **native Airflow** (Python venv) |
| **End-to-end validation** | ❓ In progress | Schema ~7 min + incremental ETL ~25 min (observed) |

**Note:** dbt is **not** a separate DAG. Schema refresh runs **inside** `etl_pipeline` (`refresh_schema_configuration`) before ETL every night.

## DAG Details

### ✅ Schema Analysis DAG (`schema_analysis_dag.py`) — optional

**Status:** Code complete — supplementary to nightly runs

**What it does:**
- Same analyzer as nightly `refresh_schema_configuration`
- Adds change detection, severity reports, and notifications

**When to run:**
- Manual: before OpenDental upgrades, when you want a changelog/Slack report
- **Not required** for nightly — `etl_pipeline` refreshes `tables.yml` every run

**Outputs:**
- `etl_pipeline/config/tables.yml`
- Backup files in `etl_pipeline/logs/schema_analysis/backups/`
- Change reports in `etl_pipeline/logs/schema_analysis/reports/`

### ✅ ETL Pipeline DAG (`etl_pipeline_dag.py`)

**Status:** Code complete — needs native Airflow + env wiring (Option A)

**What it does:**
- Business-hours guard (blocks 6 AM–8:59 PM Central)
- **Schema refresh** — backup + `analyze_opendental_schema.py` (every run)
- Validates configuration and connections
- Extracts from OpenDental → replication MySQL → PostgreSQL analytics
- Processes tables by performance category (large parallel, others sequential)
- Reports and notifies (even on partial failure)
- Runs dbt + publish when ETL succeeded (ShortCircuit otherwise)

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

**Target:** Airflow Variable `dbt_target` (`local` for laptop build; `clinic` if building on RDS)

**Future enhancements** (selectors, Cosmos, split slow marts): see [`DBT_DAG_PLAN.md`](DBT_DAG_PLAN.md)

## What's blocking production

See [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md). Summary:

1. **Phase A/B validation** — native venv smoke test (`test`), then clinic run with VPN
2. **Laptop ops** — machine on, WireGuard up, SSM tunnel open for publish, native Airflow running at 9 PM Central
3. **End-to-end run** — schema refresh → ETL → dbt → publish → notify

## Quick Start

### Option A — clinic nightly (native)

```bash
python -m venv .venv-airflow
.venv-airflow\Scripts\activate
pip install -r requirements-airflow.txt
airflow db init
airflow webserver   # separate terminal
airflow scheduler
```

Set Airflow Variables: `project_root=<repo root>`, `etl_environment=clinic`, `dbt_target=local`, `publish_environment=clinic`.

Preflight: WireGuard on; `mdc tunnel clinic-db` (port 5433). See [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md).

### Optional — Docker sandbox (test env only)

```bash
copy .env.template .env
copy etl_pipeline\.env_test.template etl_pipeline\.env_test
docker-compose build airflow-webserver airflow-scheduler
docker-compose --profile init run --rm airflow-init
docker-compose up -d postgres mysql airflow-webserver airflow-scheduler
```

Variables: `etl_environment=test`, `dbt_target=local`, `project_root=/opt/airflow/dbt_dental_clinic`.

**Not used for clinic nightly runs.**

## Manual workflow replacement

| Manual (today) | Automated (target) |
|----------------|-------------------|
| `etl-run` / `mdc etl run` | `etl_pipeline` DAG |
| `run_dbt_on_ec2.ps1 -Clinic RefreshProject` | `dbt_build` task group |
| `analyze_opendental_schema.py` before ETL | `refresh_schema_configuration` (every nightly) |
| Ad-hoc schema + change report | `schema_analysis` DAG (optional) |

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
| Where should the 9 PM clinic run execute? | **Option A — dev laptop + VPN, native Airflow** |
| First validation: test or clinic? | Phase A native test, then Phase B clinic |
| Nightly full `dbt build` vs split slow marts? | _TBD_ |

Record decisions in [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md).

## Resources

- [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md) — phased plan and gaps
- [`DEPLOYMENT_STRATEGY.md`](DEPLOYMENT_STRATEGY.md) — native Option A vs Docker sandbox
- [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md) — run semantics
- [`README.md`](README.md) — full DAG documentation
- [`DBT_DAG_PLAN.md`](DBT_DAG_PLAN.md) — future dbt orchestration options

---

**Last Updated:** 2026-06-19  
**DAG code:** 2 DAGs + integrated dbt task group — complete  
**Next:** Phase A native smoke test → Phase B clinic on laptop (Option A)
