# Airflow DAGs for OpenDental Data Pipeline

This directory contains Airflow DAG definitions for orchestrating the complete OpenDental data pipeline, including ETL, schema analysis, and dbt transformations.

## Directory Structure

```
airflow/
├── README.md                    # This file
├── ORCHESTRATION_ROADMAP.md    # Deployment roadmap, gaps, open decisions (start here)
├── DEPLOYMENT_STRATEGY.md      # Native Option A vs optional Docker sandbox
├── NIGHTLY_RUN.md              # What one nightly run is (guard → schema → ETL → dbt → publish)
├── DBT_DAG_PLAN.md             # Future dbt enhancements (selectors, Cosmos)
├── dags/
│   ├── schema_analysis_dag.py  # Schema analyzer orchestration ✅
│   ├── etl_pipeline_dag.py     # ETL pipeline orchestration ✅
│   # dbt runs inside etl_pipeline_dag (task group dbt_build)
├── tests/                      # DAG parse/import and structure tests
│   ├── README.md
│   ├── conftest.py
│   └── test_dags.py
└── plugins/                    # Custom Airflow plugins (future)
```

## DAG Overview

### 1. Schema Analysis DAG (`schema_analysis_dag.py`) — optional

**Purpose**: Same analyzer as nightly schema refresh, plus change reports and notifications.

**Schedule**: Manual trigger only (not on the nightly path)

**Tasks**:
1. Validate source connection
2. Backup existing configuration
3. Run schema analysis
4. Analyze schema changes
5. Validate new configuration
6. Send notification

**When to Run**:
- Before OpenDental upgrades (when you want a changelog/Slack report)
- Troubleshooting schema drift with detailed severity output
- **Not required nightly** — `etl_pipeline` runs `refresh_schema_configuration` every run

**Outputs**:
- `etl_pipeline/config/tables.yml`
- Backup files in `etl_pipeline/logs/schema_analysis/backups/`
- Change reports in `etl_pipeline/logs/schema_analysis/reports/`

---

### 2. ETL Pipeline DAG (`etl_pipeline_dag.py`)

**Purpose**: Extracts data from OpenDental to PostgreSQL analytics.

**Schedule**: **Nightly Mon–Sun at 9 PM Central** (requires `AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago`). See `NIGHTLY_RUN.md`.

**Flow** (in order):
0. **Schema refresh** — `refresh_schema_configuration` (backup + `analyze_opendental_schema.py`)
1. **Validation** — config freshness, DB connections, optional schema drift check
2. **ETL** — large → medium → small → tiny
3. **Verification & reporting** — verify loads, execution report, notify
4. **dbt** — only when ETL succeeded (`should_run_dbt` → `dbt_deps` → `dbt_build`)
5. **Publish** — `mdc publish analytics` when `publish_environment` is set
6. **Final notification**

**Integration**:
- **Schema**: Regenerated every run before ETL (no dependency on `schema_analysis` DAG)
- **dbt / publish**: Same DAG; ShortCircuit skips downstream when `pipeline_success` is False
- **Components**: Uses `pipeline_orchestrator.py` from etl_pipeline

**Parameters** (override when triggering):
```python
{
    "force_full_refresh": false,  # Force full refresh for all tables
    "max_workers": 5,              # Parallel workers for large tables
    "skip_validation": false       # Emergency mode (not recommended)
}
```

**Performance**:
- Small deployment: 10-30 minutes
- Medium deployment: 30-90 minutes
- Large deployment: 1-3 hours

---

### 3. dbt (inside ETL Pipeline DAG)

**Purpose**: Transforms raw data (staging → intermediate → marts).

**Schedule**: Same DAG; runs after ETL only when `pipeline_success` is True.

**Tasks**: `dbt_deps` → `dbt_build` (target from Variable `dbt_target`).

**Integration**: Same run as ETL; no separate dbt DAG. See `NIGHTLY_RUN.md` for local vs EC2.

## DAG Relationships

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          DAG ORCHESTRATION FLOW                           │
└──────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│ ETL PIPELINE DAG (Nightly 9 PM Central)                                 │
├────────────────────────────────────────────────────────────────────────┤
│ guard → schema refresh → validation → ETL → report                     │
│ → (if success) dbt_build → publish_analytics → notify                  │
├────────────────────────────────────────────────────────────────────────┤
│ Output: fresh tables.yml, raw schema loaded, marts built, RDS publish  │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│ SCHEMA ANALYSIS DAG (optional, manual)                                │
├────────────────────────────────────────────────────────────────────────┤
│ Same analyzer + change reports / Slack — not required for nightly runs │
└────────────────────────────────────────────────────────────────────────┘

FAILURE HANDLING:
━━━━━━━━━━━━━━━━
• Schema refresh fails: Run stops before ETL (no stale-config load)
• ETL fails: dbt/publish skipped; report/notify still run
• dbt/publish fails: ETL data remains in raw schema

See ORCHESTRATION_ROADMAP.md for deployment status and open decisions.
```

## Configuration

### Airflow Variables

Set these in Airflow UI (Admin → Variables):

```python
# Required (Option A clinic nightly)
etl_environment = 'clinic'
dbt_target = 'local'
project_root = 'C:/Users/rains/dbt_dental_clinic'  # repo root on disk
publish_environment = 'clinic'

# Phase A test (native or Docker sandbox)
# etl_environment = 'test'
# project_root = '/opt/airflow/dbt_dental_clinic'  # Docker sandbox mount path only

# Optional
slack_webhook_url = 'https://hooks.slack.com/services/...'
```

### Airflow Connections

Configure these connections in Airflow UI (Admin → Connections):

```python
# Slack notifications (optional)
slack_webhook
  Type: HTTP
  Host: https://hooks.slack.com/services/...
```

### Environment Variables

For **Option A (native clinic)**, the DAG sets `ETL_ENVIRONMENT` from the Airflow Variable before calling `get_settings()`. Connection details come from `etl_pipeline/.env_clinic` — same as `mdc etl run --env clinic`. Root `/.env` is not used.

For **Docker sandbox**, root `/.env` supplies Airflow metadata DB credentials only; use `etl_pipeline/.env_test` for ETL.

## Deployment

**Production (Option A):** Native Airflow on the laptop — see [`DEPLOYMENT_STRATEGY.md`](DEPLOYMENT_STRATEGY.md) and [`NIGHTLY_RUN.md`](NIGHTLY_RUN.md).

```bash
pip install -r requirements-airflow.txt
airflow db init
airflow webserver   # separate terminal
airflow scheduler
```

Set `AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago` in the environment or `airflow.cfg`.

### Optional: Docker Compose sandbox (test env)

Not used for clinic nightly runs. See `docker-compose.yml` comments and [`DEPLOYMENT_STRATEGY.md`](DEPLOYMENT_STRATEGY.md).

```bash
docker-compose --profile init run --rm airflow-init
docker-compose up -d postgres mysql airflow-webserver airflow-scheduler
# UI: http://localhost:8080
```

### Production paths (deferred)

EC2 or managed Airflow (MWAA) — same DAGs; revisit if moving off Option A. Set `project_root` Variable to the deployed repo path.

## Running the DAGs

### Via Airflow UI

1. Navigate to http://localhost:8080 (or your Airflow URL)
2. Enable the DAG by toggling the switch
3. Trigger manually or wait for scheduled run
4. Monitor progress in Graph View or Task Instance logs

### Via CLI

```bash
# Trigger schema analysis
airflow dags trigger schema_analysis

# Trigger with config
airflow dags trigger schema_analysis \
  --conf '{"environment": "test"}'

# List DAG runs
airflow dags list-runs -d schema_analysis

# View task logs
airflow tasks logs schema_analysis run_schema_analysis <execution_date>
```

### Via API (Programmatic)

```python
import requests

# Trigger DAG via Airflow REST API
response = requests.post(
    'http://localhost:8080/api/v1/dags/schema_analysis/dagRuns',
    auth=('airflow', 'airflow'),
    json={'conf': {'environment': 'clinic'}}
)
```

## Monitoring & Alerts

### Email Notifications

Configure SMTP in `airflow.cfg` or docker-compose.yml:

```bash
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_USER=your-email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your-app-password
AIRFLOW__SMTP__SMTP_MAIL_FROM=airflow@example.com
```

### Slack Notifications

1. Create Slack webhook: https://api.slack.com/messaging/webhooks
2. Set Airflow Variable: `slack_webhook_url`
3. Notifications sent automatically on DAG completion

### Logs

View logs in:
- **Airflow UI**: Task Instance → Logs
- **File System**: `logs/scheduler/` and `logs/dag_processor/`
- **Application Logs**: `etl_pipeline/logs/etl_pipeline/` for ETL run logs; `etl_pipeline/logs/schema_analysis/` for schema artifacts

## Best Practices

### 1. Schema Analysis Workflow

```bash
# Typical workflow:
1. Schema Analysis DAG runs weekly (scheduled)
2. Review notification email/Slack
3. If changes detected:
   a. Review changelog in etl_pipeline/logs/schema_analysis/reports/
   b. Test ETL pipeline in test environment
   c. Update dbt models if needed
   d. Run production ETL pipeline
```

### 2. Configuration Management

- **Version Control**: Commit `tables.yml` changes to git
- **Change Review**: Review schema changes before production deployment
- **Backup Strategy**: Keep backups for rollback capability
- **Environment Separation**: Separate clinic and test configurations

### 3. Error Handling

- **Retries**: DAGs configured with 2 retries, 5-minute delay
- **Notifications**: Email on failure, no email on retry
- **Timeouts**: 30-minute timeout for schema analysis
- **Graceful Degradation**: Individual task failures don't stop dependent tasks

### 4. Performance

- **Parallel Tasks**: Large tables processed in parallel
- **Resource Limits**: One schema analysis at a time (`max_active_runs=1`)
- **Connection Pooling**: Proper connection management in tasks
- **Timeout Protection**: All expensive operations have timeouts

## Troubleshooting

### Common Issues

#### 1. DAG Not Appearing in UI

```bash
# Check DAG for syntax errors
python airflow/dags/schema_analysis_dag.py

# Check Airflow logs
tail -f logs/scheduler/latest/schema_analysis_dag.py.log
```

#### 2. Import Errors

```bash
# Ensure ETL pipeline is accessible
export PYTHONPATH=/opt/airflow/dbt_dental_clinic:$PYTHONPATH

# Or add to docker-compose.yml:
environment:
  - PYTHONPATH=/opt/airflow/dbt_dental_clinic
```

#### 3. Connection Failures

```bash
# Test database connections
python -m etl_pipeline.cli test-connections

# Check environment variables
echo $ETL_ENVIRONMENT
```

#### 4. Permission Errors

```bash
# Ensure Airflow user has write access
chmod -R 755 logs/
chown -R airflow:airflow logs/
```

### Debug Mode

Enable detailed logging in DAG:

```python
import logging
logging.getLogger('etl_pipeline').setLevel(logging.DEBUG)
```

## Development

### Adding New DAGs

1. Create DAG file in `airflow/dags/`
2. Follow naming convention: `<purpose>_dag.py`
3. Include comprehensive documentation
4. Test locally before deploying
5. Add to this README

### Testing DAGs

```bash
# Parse DAG (check for errors)
python airflow/dags/schema_analysis_dag.py

# Test specific task
airflow tasks test schema_analysis validate_source_connection 2025-01-01

# Dry run
airflow dags test schema_analysis 2025-01-01
```

## Migration Path

| Phase | State | What runs |
|-------|-------|-----------|
| **Today (manual)** | Default | `etl-run` / `mdc etl run`, `run_dbt_on_ec2.ps1`, ad-hoc schema analysis |
| **Code complete** | DAGs implemented | `schema_analysis`, `etl_pipeline` (+ integrated `dbt_build` task group) |
| **Target** | Deployed + validated | Nightly unattended on laptop (native Airflow + VPN); manual scripts as break-glass |

**Blockers to target:** native Airflow install, end-to-end smoke test (Phase A/B), tunnel + VPN ops checklist. Orchestration host decided: Option A.

Full phased plan and open decisions: [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md).

## Resources

- **Orchestration roadmap**: [`ORCHESTRATION_ROADMAP.md`](ORCHESTRATION_ROADMAP.md) — gaps, phased plan, open decisions
- **Airflow Documentation**: https://airflow.apache.org/docs/
- **Best Practices**: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- **Astronomer Cosmos** (dbt): https://astronomer.github.io/astronomer-cosmos/

## Support

For questions or issues:
1. Check DAG logs in Airflow UI
2. Review application logs in `etl_pipeline/logs/etl_pipeline/`
3. Consult team documentation
4. Contact data engineering team

---

**Last Updated**: 2026-06-17
**Maintained By**: Data Engineering Team

