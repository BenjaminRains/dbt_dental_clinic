# Nightly Pipeline Guide

This document explains the nightly ETL pipeline approach and how to manually execute it using the CLI.

## Pipeline Architecture

The ETL pipeline uses an **orchestrated approach** where all stages run as part of a single process:

### **Pipeline Flow**
```
1. EXTRACT → 2. LOAD → 3. TRANSFORM
   ↓           ↓         ↓
   Concurrent execution within single orchestration
```

### **Stage Execution**
- **Extract**: Copy data from OpenDental production to replication
- **Load**: Load data from replication to analytics raw schema  
- **Transform**: Transform data from raw to public schema

**Note**: These stages run **concurrently** as part of a single orchestrated pipeline, not at separate scheduled times.

## Manual Execution

Since the pipeline is not automatically scheduled, you need to run it manually using the CLI:

### **Run Full Pipeline**
```bash
# Run the complete ETL pipeline for all tables
python -m etl_pipeline.cli.main run

# Run with specific configuration file
python -m etl_pipeline.cli.main run --config etl_pipeline/config/pipeline.yml

# Force full refresh (ignore incremental settings)
python -m etl_pipeline.cli.main run --force

# Run with fewer parallel workers (default: 2)
python -m etl_pipeline.cli.main run --parallel 1
```

### **Run Specific Tables**
```bash
# Run pipeline for specific tables only
python -m etl_pipeline.cli.main run --tables patients appointments procedures

# Run with full refresh for specific tables
python -m etl_pipeline.cli.main run --tables patients --force
```

### **Dry Run (Preview)**
```bash
# See what would be done without making changes
python -m etl_pipeline.cli.main run --dry-run
```

## Pipeline Stages

### **1. Extract Stage**
- **Purpose**: Copy data from OpenDental production to local replication
- **Source**: `opendental_source` (production database)
- **Target**: `opendental_replication` (local replica)
- **Method**: Exact MySQL replication with schema validation
- **Timeout**: 30 minutes

### **2. Load Stage**
- **Purpose**: Load data from replication to analytics raw schema
- **Source**: `opendental_replication` (local replica)
- **Target**: `opendental_analytics_raw` (analytics raw schema)
- **Method**: Bulk load with data type validation
- **Timeout**: 45 minutes

### **3. Transform Stage**
- **Purpose**: Transform data from raw to public schema
- **Source**: `opendental_analytics_raw` (raw schema)
- **Target**: `opendental_analytics_public` (public schema)
- **Method**: Business logic transformations and data cleaning
- **Timeout**: 60 minutes

## Orchestration Process

The `PipelineOrchestrator` coordinates the entire process:

1. **Initialize Connections**: Set up all database connections
2. **Process Tables**: For each table, run the complete ETL pipeline:
   - Extract → Load → Transform (concurrent execution)
3. **Priority Processing**: Process tables by importance level
4. **Parallel Execution**: Use multiple workers for efficiency
5. **Error Handling**: Retry failed operations and log errors

## Configuration Settings

### **Performance Settings**
```yaml
general:
  batch_size: 5000      # Larger batches for nightly runs
  parallel_jobs: 2      # Fewer parallel jobs for manual execution
  max_retries: 3        # Retry failed operations
  retry_delay_seconds: 300  # 5 minutes between retries
```

### **Stage Timeouts**
```yaml
stages:
  extract:
    timeout_minutes: 30
  load:
    timeout_minutes: 45
  transform:
    timeout_minutes: 60
```

### **Monitoring Settings**
```yaml
monitoring:
  metrics:
    collection_interval: 300  # 5 minutes (during pipeline runs)
    retention_days: 30
```

## Best Practices

### **1. Execution Timing**
- **Recommended**: Run during off-hours (1:00 AM - 4:00 AM)
- **Avoid**: Running during business hours when OpenDental is active
- **Duration**: Expect 2-3 hours for full pipeline execution

### **2. Monitoring**
```bash
# Check pipeline status
python -m etl_pipeline.cli.main status

# Test database connections
python -m etl_pipeline.cli.main test-connections

# Validate configuration
python -m etl_pipeline.cli.main config validate
```

### **3. Error Handling**
- **Automatic retries**: Failed operations retry up to 3 times
- **Error logging**: All errors logged to `logs/errors.log`
- **Notifications**: Email/Slack alerts for critical failures

### **4. Data Validation**
- **Row count checks**: Verify data integrity between stages
- **Schema validation**: Ensure table structures match
- **Data type validation**: Validate data types during load

## Troubleshooting

### **Common Issues**

1. **Connection Failures**
   ```bash
   # Test all connections
   python -m etl_pipeline.cli.main test-connections
   ```

2. **Memory Issues**
   ```bash
   # Reduce parallel jobs
   python -m etl_pipeline.cli.main run --parallel 1
   ```

3. **Timeout Issues**
   ```bash
   # Check pipeline configuration timeouts
   python -m etl_pipeline.cli.main config show --section stages
   ```

### **Log Files**
- **Pipeline logs**: `logs/pipeline.log`
- **Error logs**: `logs/errors.log`
- **Schema discovery**: `logs/schema_discovery_*.log`

## Automation Options

### **Manual Scheduling**
- **Windows Task Scheduler**: Create scheduled task for nightly execution
- **Linux Cron**: Add cron job for automated execution
- **Airflow**: Use existing Airflow DAGs for orchestration

### **Monitoring Integration**
- **Slack**: Receive notifications in `#dental-clinic-alerts`
- **Email**: Get email alerts for failures
- **Metrics**: Monitor pipeline performance over time

## Performance Optimization

### **For Large Datasets**
```bash
# Increase batch size for faster processing
python -m etl_pipeline.cli.main run --config custom_pipeline.yml

# Process tables in priority order
python -m etl_pipeline.cli.main run --tables critical_table1 critical_table2
```

### **For Development/Testing**
```bash
# Run with smaller batches
python -m etl_pipeline.cli.main run --tables test_table --dry-run

# Test specific stages
python -m etl_pipeline.cli.main run --tables small_table --force
```

## Security Considerations

- **Database credentials**: Stored in environment variables
- **Read-only access**: Source database connections are read-only
- **Audit logging**: All operations logged for compliance
- **Error handling**: Sensitive data not exposed in error messages 