# Airflow DAGs Status

Quick reference for the status of all Airflow DAGs in the OpenDental data pipeline.

## Implementation Status

| DAG | Status | Schedule | Purpose | Dependencies |
|-----|--------|----------|---------|--------------|
| **schema_analysis** | ✅ Complete | Weekly (Sun 2 AM) | Analyze schema, generate tables.yml | OpenDental source DB |
| **etl_pipeline** | ✅ Complete | Daily (3 AM) | Extract & load data | tables.yml, All DBs |
| **dbt_build** | 📋 Planned | After ETL | Transform data | PostgreSQL raw schema |

## DAG Details

### ✅ Schema Analysis DAG (`schema_analysis_dag.py`)

**Status**: Production Ready

**What it does**:
- Analyzes OpenDental MySQL schema
- Generates `tables.yml` configuration
- Detects breaking changes
- Sends notifications with severity levels

**When to run**:
- Scheduled: Weekly on Sunday at 9 PM (Central time)
- Manual: Before OpenDental upgrades
- Manual: When schema changes suspected
- Manual: When ETL fails with schema errors

**Key Features**:
- Automatic backup of existing config
- Schema change detection (SCD)
- Breaking change alerts
- Configuration validation

**Outputs**:
- `etl_pipeline/config/tables.yml`
- Backup files in `logs/schema_analysis/backups/`
- Change reports in `logs/schema_analysis/reports/`

### ✅ ETL Pipeline DAG (`etl_pipeline_dag.py`)

**Status**: Production Ready

**What it does**:
- Validates configuration and connections
- Extracts from OpenDental to MySQL replication
- Loads from MySQL replication to PostgreSQL analytics
- Processes tables by performance category
- Generates execution reports

**When to run**:
- Scheduled: Daily at 9PM (localhost)
- Can adjust to hourly for real-time needs
- Manual: On-demand data refresh
- Manual: After schema changes

**Key Features**:
- Configuration age validation
- Schema drift detection
- Parallel processing for large tables (5 workers)
- Sequential processing for smaller tables
- Comprehensive error handling
- Per-table retry capability
- Detailed execution reports

**Outputs**:
- PostgreSQL analytics raw schema (populated)
- Execution reports
- Performance metrics

**Parameters** (when triggering manually):
```python
{
    "force_full_refresh": false,  # Force full refresh for all tables
    "max_workers": 5,              # Parallel workers for large tables  
    "skip_validation": false       # Emergency mode (not recommended)
}
```

### 📋 dbt Build DAG (Planned)

**Status**: To Be Created

**What it will do**:
- Run dbt deps
- Check source freshness
- Transform raw data (staging → intermediate → marts)
- Run dbt tests
- Generate documentation

**Trigger**: After successful ETL Pipeline completion

See `DBT_DAG_PLAN.md` for implementation details.

## Quick Start

### 1. Deploy to Airflow

```bash
# Copy DAGs to Airflow
cp airflow/dags/*.py /opt/airflow/dags/

# Or mount via docker-compose.yml
volumes:
  - ./airflow/dags:/opt/airflow/dags
  - ./etl_pipeline:/opt/airflow/dbt_dental_clinic/etl_pipeline
```

### 2. Set Airflow Variables

```bash
# Required
airflow variables set etl_environment clinic

# Optional (for notifications)
airflow variables set slack_webhook_url https://hooks.slack.com/services/...
```

### 3. Enable DAGs

Navigate to Airflow UI (http://localhost:8080) and toggle:
- `schema_analysis` → ON
- `etl_pipeline` → ON

### 4. Initial Run

```bash
# First, run schema analysis to generate tables.yml
airflow dags trigger schema_analysis

# Wait for completion, then run ETL
airflow dags trigger etl_pipeline
```

## Environment-Specific Configuration

### Development/Test

```python
# Set in Airflow Variables
etl_environment = 'test'

# Points to test databases (.env_test)
```

### Production

```python
# Set in Airflow Variables
etl_environment = 'clinic'

# Points to clinic databases (.env_clinic)
```

## Monitoring

### Check DAG Status

```bash
# List all DAGs
airflow dags list

# Check specific DAG runs
airflow dags list-runs -d schema_analysis
airflow dags list-runs -d etl_pipeline

# View task logs
airflow tasks logs schema_analysis run_schema_analysis <execution_date>
airflow tasks logs etl_pipeline process_large_tables <execution_date>
```

### View in Airflow UI

1. Navigate to http://localhost:8080
2. Click on DAG name
3. View:
   - **Graph View**: Task dependencies
   - **Tree View**: Historical runs
   - **Task Instance**: Individual task logs

### Notifications

Both DAGs send notifications via:
- ✉️ **Email**: If SMTP configured in Airflow
- 💬 **Slack**: If webhook URL configured
- 📋 **Logs**: Always available in Airflow UI

## Typical Workflow

### Weekly Routine

**Sunday 2 AM**: Schema Analysis DAG runs
- Analyzes schema
- Detects changes
- Updates tables.yml
- Sends notification

**Monday-Sunday 3 AM**: ETL Pipeline DAG runs
- Validates configuration
- Processes new/changed data
- Loads to analytics database
- Sends success/failure report

### After OpenDental Upgrade

```bash
# 1. Run schema analysis to detect changes
airflow dags trigger schema_analysis

# 2. Review change report (check email/Slack)

# 3. If breaking changes, update code/models

# 4. Test ETL in test environment
airflow dags trigger etl_pipeline --conf '{"force_full_refresh": true}'

# 5. Deploy to production if successful
```

### Troubleshooting Failed Tables

```bash
# 1. Check execution report
# Look for failed_tables in logs

# 2. Review individual table logs
# Airflow UI → etl_pipeline → process_*_tables → Logs

# 3. Retry specific table manually
python -m etl_pipeline.cli run --table <table_name>

# 4. Or force full refresh for that table
python -m etl_pipeline.cli run --table <table_name> --force-full
```

## Performance Expectations

### Schema Analysis DAG

| Database Size | Expected Duration |
|---------------|-------------------|
| Small (<100 tables) | 1-2 minutes |
| Medium (100-300 tables) | 2-5 minutes |
| Large (300+ tables) | 5-10 minutes |

### ETL Pipeline DAG

| Data Volume | Expected Duration |
|-------------|-------------------|
| Small (<1M total rows) | 10-30 minutes |
| Medium (1M-10M rows) | 30-90 minutes |
| Large (10M+ rows) | 1-3 hours |

**Note**: First run (full refresh) takes longer than incremental runs.

## Error Codes & Solutions

### Schema Analysis DAG

| Error | Cause | Solution |
|-------|-------|----------|
| Connection timeout | Source DB unavailable | Check network, credentials |
| Schema hash mismatch | OpenDental upgraded | Expected, review changes |
| Breaking changes detected | Tables/columns removed | Update ETL/dbt models |

### ETL Pipeline DAG

| Error | Cause | Solution |
|-------|-------|----------|
| Config too old (>90 days) | tables.yml outdated | Run Schema Analysis DAG |
| Connection failures | DB credentials/network | Check connections |
| Large table failures | Timeout/resources | Increase timeout, workers |
| Schema drift warning | Schema changed | Run Schema Analysis DAG |

## Next Steps

1. ✅ **Schema Analysis DAG** - Complete
2. ✅ **ETL Pipeline DAG** - Complete
3. 📋 **dbt Build DAG** - Create next
4. 📋 **Monitoring Dashboard** - Setup (Grafana/CloudWatch)
5. 📋 **Data Quality Checks** - Implement (Great Expectations)

## Resources

- **Airflow Docs**: https://airflow.apache.org/docs/
- **ETL Pipeline Architecture**: `etl_pipeline/docs/PIPELINE_ARCHITECTURE.md`
- **dbt DAG Plan**: `airflow/DBT_DAG_PLAN.md`
- **Airflow Setup**: `airflow/README.md`

---

**Last Updated**: 2025-10-22  
**Status**: 2/3 DAGs Complete  
**Next**: dbt Build DAG

