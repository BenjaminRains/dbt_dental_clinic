# Airflow DAGs for OpenDental Data Pipeline

This directory contains Airflow DAG definitions for orchestrating the complete OpenDental data pipeline, including ETL, schema analysis, and dbt transformations.

## Directory Structure

```
airflow/
├── README.md                    # This file
├── DEPLOYMENT_STRATEGY.md      # How we deploy Airflow; how Docker fits in
├── DBT_DAG_PLAN.md             # dbt DAG implementation plan
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

### 1. Schema Analysis DAG (`schema_analysis_dag.py`)

**Purpose**: Analyzes OpenDental schema and generates `tables.yml` configuration.

**Schedule**: Weekly (Sunday 2 AM) or on-demand

**Tasks**:
1. Validate source connection
2. Backup existing configuration
3. Run schema analysis
4. Analyze schema changes
5. Validate new configuration
6. Send notification

**When to Run**:
- Scheduled: Weekly/monthly for ongoing schema monitoring
- Manual: Before OpenDental upgrades
- Event-based: When schema changes are expected
- Troubleshooting: When ETL fails with schema errors

**Outputs**:
- `etl_pipeline/config/tables.yml` (primary configuration)
- Backup files in `logs/schema_analysis/backups/`
- Change reports in `logs/schema_analysis/reports/`

---

### 2. ETL Pipeline DAG (`etl_pipeline_dag.py`)

**Purpose**: Extracts data from OpenDental to PostgreSQL analytics.

**Schedule**: **Nightly Mon–Sun at 9 PM Central** (requires `AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Chicago`). See `NIGHTLY_RUN.md` for what a run is (incremental ETL + dbt on success).

**Task Groups**:

1. **Validation**
   - Validate configuration (tables.yml freshness check)
   - Validate database connections (source, replication, analytics)
   - Check schema drift (optional)

2. **ETL Processing** (by performance category)
   - Process large tables (1M+ rows) - **Parallel** with 5 workers
   - Process medium tables (100K-1M rows) - Sequential
   - Process small tables (10K-100K rows) - Sequential
   - Process tiny tables (<10K rows) - Sequential

3. **Verification & Reporting**
   - Verify loads (row counts, timestamps)
   - Generate execution report
   - Send completion notification

4. **dbt** (only when ETL succeeded)
   - Short-circuit skips dbt if `pipeline_success` is False
   - `dbt deps` then `dbt build` (target from Variable `dbt_target`, default `local`)

**Integration**:
- **Upstream**: Depends on valid `tables.yml` from Schema Analysis DAG
- **dbt**: Same DAG; runs after reporting only when ETL succeeded
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
│ 1. SCHEMA ANALYSIS DAG (Weekly / On-Demand)                           │
├────────────────────────────────────────────────────────────────────────┤
│ Tasks:                                                                 │
│  ✓ Validate source connection                                         │
│  ✓ Backup existing config                                             │
│  ✓ Run schema analysis                                                │
│  ✓ Detect schema changes                                              │
│  ✓ Validate new config                                                │
│  ✓ Send notification (with severity: INFO/WARNING/CRITICAL)           │
├────────────────────────────────────────────────────────────────────────┤
│ Output: tables.yml (configuration for ETL)                             │
└────────────────────────┬───────────────────────────────────────────────┘
                         │
                         ↓ Configuration file (versioned)
                         │
┌────────────────────────┴───────────────────────────────────────────────┐
│ 2. ETL PIPELINE DAG (Daily at 3 AM)                                   │
├────────────────────────────────────────────────────────────────────────┤
│ Validation Phase:                                                      │
│  ✓ Validate tables.yml (age, environment, structure)                  │
│  ✓ Test database connections (source, replication, analytics)         │
│  ✓ Check schema drift (optional quick check)                          │
│                                                                        │
│ Processing Phase (by performance category):                            │
│  ✓ Large tables (1M+ rows)    → PARALLEL (5 workers)                  │
│  ✓ Medium tables (100K-1M)    → Sequential                            │
│  ✓ Small tables (10K-100K)    → Sequential                            │
│  ✓ Tiny tables (<10K)         → Sequential                            │
│                                                                        │
│ Reporting Phase:                                                       │
│  ✓ Verify loads (row counts, timestamps)                              │
│  ✓ Generate execution report                                          │
│  ✓ Send notification (SUCCESS/WARNING/ERROR)                          │
├────────────────────────────────────────────────────────────────────────┤
│ Output: PostgreSQL Analytics (raw schema populated)                    │
└────────────────────────┬───────────────────────────────────────────────┘
                         │
                         ↓ Raw data ready for transformation
                         │
┌────────────────────────┴───────────────────────────────────────────────┐
│ 3. DBT BUILD DAG (Triggered on ETL success) [TO BE CREATED]           │
├────────────────────────────────────────────────────────────────────────┤
│ Tasks (planned):                                                       │
│  ✓ dbt deps                                                            │
│  ✓ dbt source freshness                                               │
│  ✓ dbt run (staging → intermediate → marts)                           │
│  ✓ dbt test                                                            │
│  ✓ dbt docs generate                                                   │
├────────────────────────────────────────────────────────────────────────┤
│ Output: Analytics marts ready for consumption                          │
└────────────────────────────────────────────────────────────────────────┘

COMMUNICATION BETWEEN DAGS:
━━━━━━━━━━━━━━━━━━━━━━━━
• Schema Analysis → ETL Pipeline: Via tables.yml (static configuration)
• ETL Pipeline → dbt Build: Via Airflow trigger (dynamic)
• All DAGs: Email/Slack notifications with severity levels

FAILURE HANDLING:
━━━━━━━━━━━━━━━━
• Schema Analysis fails: Alerts sent, ETL can still run with old config
• ETL Pipeline fails: Individual table failures logged, retries available
• dbt Build fails: Model-level granularity, partial success possible
```

## Configuration

### Airflow Variables

Set these in Airflow UI (Admin → Variables):

```python
# Required
etl_environment = 'production'  # or 'test'

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

Ensure these are set in your Airflow environment:

```bash
# Set in docker-compose.yml or Airflow configuration
ETL_ENVIRONMENT=production

# Or mount .env files to Airflow containers
# See etl_pipeline/.env_production and etl_pipeline/.env_test
```

## Deployment

### Local Development (Docker Compose)

```bash
# Start Airflow
docker-compose up airflow-init
docker-compose up

# Access Airflow UI
# http://localhost:8080
# Default: airflow/airflow

# Mount this directory as /opt/airflow/dags in docker-compose.yml
volumes:
  - ./airflow/dags:/opt/airflow/dags
  - ./etl_pipeline:/opt/airflow/dbt_dental_clinic/etl_pipeline
  - ./dbt_dental_models:/opt/airflow/dbt_dental_clinic/dbt_dental_models
```

### Production Deployment

Update paths in DAG files:

```python
# In schema_analysis_dag.py, update:
PROJECT_ROOT = Path('/opt/airflow/dbt_dental_clinic')

# Or use Airflow Variables for flexibility:
PROJECT_ROOT = Path(Variable.get('project_root', '/opt/airflow/dbt_dental_clinic'))
```

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
    json={'conf': {'environment': 'production'}}
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
- **Application Logs**: `etl_pipeline/logs/` for ETL-specific logs

## Best Practices

### 1. Schema Analysis Workflow

```bash
# Typical workflow:
1. Schema Analysis DAG runs weekly (scheduled)
2. Review notification email/Slack
3. If changes detected:
   a. Review changelog in logs/schema_analysis/reports/
   b. Test ETL pipeline in test environment
   c. Update dbt models if needed
   d. Run production ETL pipeline
```

### 2. Configuration Management

- **Version Control**: Commit `tables.yml` changes to git
- **Change Review**: Review schema changes before production deployment
- **Backup Strategy**: Keep backups for rollback capability
- **Environment Separation**: Separate production and test configurations

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

### Current State (Manual)
```bash
# Manual schema analysis
python etl_pipeline/scripts/analyze_opendental_schema.py

# Manual ETL run
python -m etl_pipeline.cli run --all
```

### Phase 1 (Schema Analysis DAG)
```bash
# Airflow manages schema analysis
# Still manual ETL
```

### Phase 2 (ETL Pipeline DAG)
```bash
# Airflow manages both schema analysis and ETL
# Still manual dbt
```

### Phase 3 (Complete Orchestration)
```bash
# Airflow manages everything:
# - Schema analysis (weekly)
# - ETL pipeline (daily)
# - dbt transformations (after ETL)
```

## Resources

- **Airflow Documentation**: https://airflow.apache.org/docs/
- **Best Practices**: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- **Astronomer Cosmos** (dbt): https://astronomer.github.io/astronomer-cosmos/

## Support

For questions or issues:
1. Check DAG logs in Airflow UI
2. Review application logs in `etl_pipeline/logs/`
3. Consult team documentation
4. Contact data engineering team

---

**Last Updated**: 2025-10-22
**Maintained By**: Data Engineering Team

