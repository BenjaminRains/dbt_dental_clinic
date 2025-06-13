# ETL Pipeline Implementation Guide
## Based on Schema Discovery Analysis

**Goal**: Transform from manual, hardcoded ETL to intelligent, auto-discovering system  
**Timeline**: 4-week phased implementation  
**Status**: Ready to begin Phase 1

---

## Quick Start: Top 5 Critical Tables

Start with these high-impact tables to validate the approach:

```yaml
Priority 1 Tables:
1. securitylog     (705 MB, 3.7M rows) - Audit & Compliance
2. paysplit        (386 MB, 1.4M rows) - Payment Allocations  
3. procedurelog    (361 MB, 745K rows) - Clinical Procedures
4. histappointment (344 MB, 571K rows) - Appointment History
5. sheetfield      (296 MB, 1.6M rows) - Form Data
```

---

## Phase 1: Foundation Setup (Week 1)

### Step 1: Update Pipeline Configuration

Replace hardcoded table lists with intelligent configuration:

```python
# OLD: etl_pipeline.py hardcoded approach
TABLES_TO_PROCESS = ["patient", "appointment", "payment"]  # Manual list

# NEW: Use generated configuration
import yaml
with open('etl_pipeline/config/tables.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Get tables by priority
critical_tables = [name for name, cfg in config['source_tables'].items() 
                  if cfg['table_importance'] == 'critical']
```

### Step 2: Implement Priority-Based Processing

```python
def process_tables_by_priority():
    """Process tables in order of business importance."""
    
    priorities = ['critical', 'important', 'audit', 'reference']
    
    for priority in priorities:
        tables = get_tables_by_importance(priority)
        
        logger.info(f"Processing {len(tables)} {priority} tables")
        
        # Process critical tables with parallel execution
        if priority == 'critical':
            process_in_parallel(tables, max_workers=3)
        else:
            process_sequentially(tables)
```

### Step 3: Smart Batch Sizing

```python
def get_optimal_batch_size(table_name: str) -> int:
    """Get intelligent batch size based on table characteristics."""
    table_config = config['source_tables'][table_name]
    return table_config['batch_size']  # Already calculated!

def extract_table(table_name: str):
    batch_size = get_optimal_batch_size(table_name)
    # Use the intelligent batch size
    return extract_in_batches(table_name, batch_size)
```

### Step 4: Incremental Extraction

```python
def should_use_incremental(table_name: str) -> bool:
    """Check if table supports incremental extraction."""
    table_config = config['source_tables'][table_name]
    return table_config['extraction_strategy'] == 'incremental'

def get_incremental_column(table_name: str) -> str:
    """Get the timestamp column for incremental extraction."""
    table_config = config['source_tables'][table_name]
    return table_config['incremental_column']

def extract_incremental(table_name: str, last_run_time: datetime):
    """Extract only new/modified records."""
    if should_use_incremental(table_name):
        column = get_incremental_column(table_name)
        sql = f"""
        SELECT * FROM {table_name} 
        WHERE {column} > '{last_run_time}'
        ORDER BY {column}
        """
        return execute_query(sql)
    else:
        return extract_full_table(table_name)
```

---

## Phase 2: Business Tables (Week 2)

### Add Important Tables Processing

```python
# Process the 97 important tables
important_tables = [name for name, cfg in config['source_tables'].items() 
                   if cfg['table_importance'] == 'important']

# These include:
# - claim, claimproc, claimpayment (Insurance processing)
# - employee, employer (HR data)  
# - medicationpat, allergy (Clinical data)
# - recall, recalltrigger (Patient management)
```

### Implement Advanced Monitoring

```python
def setup_monitoring(table_name: str):
    """Configure monitoring based on table importance."""
    table_config = config['source_tables'][table_name]
    monitoring = table_config['monitoring']
    
    return MonitoringConfig(
        alert_on_failure=monitoring['alert_on_failure'],
        max_time_minutes=monitoring['max_extraction_time_minutes'],
        quality_threshold=monitoring['data_quality_threshold']
    )
```

---

## Phase 3: Audit & Compliance (Week 3)

### Add Audit Tables (84 tables)

Focus on compliance and tracking:
- `activeinstance` - System monitoring
- `sessiontoken` - Security tracking  
- `updatehistory` - Change tracking
- `tsitranslog` - Transaction logs

### Implement Compliance Features

```python
def ensure_audit_trail(table_name: str):
    """Ensure audit tables maintain complete history."""
    if is_audit_table(table_name):
        # Never truncate audit tables
        # Implement retention policies
        # Ensure data integrity
        pass
```

---

## Phase 4: Complete System (Week 4)

### Add Reference Tables (220 tables)

Batch process all lookup and configuration tables:
- All `*def` tables (definitions)
- All `*type` tables (types)  
- All `*pref` tables (preferences)
- Geographic and code tables

### Performance Optimization

```python
def optimize_reference_processing():
    """Batch process small reference tables efficiently."""
    reference_tables = get_reference_tables()
    
    # Group small tables for batch processing
    small_tables = [t for t in reference_tables 
                   if get_table_size(t) < 1]  # < 1MB
    
    process_table_batch(small_tables)
```

---

## Monitoring Dashboard Setup

### Critical Metrics to Track

```yaml
Real-Time Alerts:
  - Critical table failures (immediate)
  - Processing time overruns (15 min)
  - Data quality below threshold (immediate)

Daily Reports:
  - Tables processed successfully
  - Data volume transferred  
  - Performance trends
  - Error summary

Weekly Reviews:
  - Processing time optimization
  - Data quality trends
  - System utilization
  - Capacity planning
```

### Alerting Configuration

```python
def setup_alerting():
    """Configure tiered alerting based on table importance."""
    
    # Critical: Immediate PagerDuty + Email
    critical_alert = AlertConfig(
        channels=['pagerduty', 'email', 'slack'],
        escalation_minutes=15,
        auto_retry=True
    )
    
    # Important: Email within 15 minutes
    important_alert = AlertConfig(
        channels=['email', 'slack'],
        escalation_minutes=60,
        auto_retry=True
    )
    
    # Audit/Reference: Daily summary only
    standard_alert = AlertConfig(
        channels=['email'],
        escalation_hours=24,
        auto_retry=False
    )
```

---

## Validation & Testing Strategy

### Phase 1 Testing

```bash
# Test critical tables only
python -m etl_pipeline.main --tables=critical --dry-run

# Validate configuration
python -m etl_pipeline.scripts.validate_config

# Monitor first run
python -m etl_pipeline.monitor --watch=critical
```

### Data Quality Validation

```python
def validate_extraction(table_name: str):
    """Validate data quality after extraction."""
    expected_threshold = get_quality_threshold(table_name)
    
    # Row count validation
    source_count = count_source_rows(table_name)
    target_count = count_target_rows(table_name)
    
    quality_score = target_count / source_count
    
    if quality_score < expected_threshold:
        raise DataQualityError(f"{table_name} quality below threshold")
```

---

## Integration with Existing Pipeline

### Gradual Migration Strategy

```python
# Week 1: Run both old and new pipelines in parallel
def hybrid_extraction():
    # Old pipeline for non-critical tables
    run_legacy_pipeline(exclude=critical_tables)
    
    # New pipeline for critical tables only
    run_intelligent_pipeline(include=critical_tables)
    
    # Compare results and validate

# Week 2-4: Gradually migrate table groups
def migrate_table_group(importance_level: str):
    # Move table group to new pipeline
    # Validate results
    # Disable old pipeline for those tables
```

### Rollback Plan

```python
def emergency_rollback():
    """Quick rollback to legacy pipeline if needed."""
    # Disable new pipeline
    # Re-enable legacy pipeline
    # Alert operations team
    # Generate incident report
```

---

## Success Metrics

### Week 1 Targets
- [ ] 5 critical tables processing successfully
- [ ] Monitoring alerts functioning
- [ ] Performance within SLA (5-70 minutes per table)
- [ ] Data quality > 99% for critical tables

### Week 2 Targets  
- [ ] 128 tables total (31 critical + 97 important)
- [ ] Incremental extraction working for 60 tables
- [ ] Automated failure recovery functioning
- [ ] Processing time optimized

### Week 3 Targets
- [ ] 212 tables total (adding 84 audit tables)
- [ ] Compliance reporting functional
- [ ] Off-peak processing scheduled
- [ ] Historical data validation complete

### Week 4 Targets
- [ ] All 432 tables processing
- [ ] Full data warehouse populated
- [ ] Performance optimized and stable
- [ ] Documentation and runbooks complete

---

## Next Actions

1. **Today**: Review this guide and `tables.yaml` configuration
2. **Tomorrow**: Begin Phase 1 implementation with critical tables
3. **Week 1**: Complete foundation setup and testing
4. **Ongoing**: Follow 4-week implementation roadmap

The intelligent configuration is ready - time to put it to work! 🚀 