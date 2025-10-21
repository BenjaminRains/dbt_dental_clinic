# ETL Pipeline Monitoring Guide: HYBRID FIX

**Created:** October 21, 2025  
**Purpose:** Monitor the HYBRID FIX for stale state detection and recovery  
**Related:** Bug fix for incremental loading stale state issue

---

## What to Monitor

The HYBRID FIX is a self-healing mechanism that automatically detects when:
- Incremental queries return 0 rows
- But analytics database has fewer rows than replication database
- Falls back to full load to recover missing data

**This should be RARE.** Frequent triggers indicate a recurring problem.

---

## Daily Monitoring (5 minutes)

### Check 1: Method Tracking Data

**After each ETL run:**

```bash
cd etl_pipeline
cat logs/method_usage.json | grep -A5 "_check_analytics_needs_updating"
```

**Expected Output:**
```json
"PostgresLoader._check_analytics_needs_updating": {
  "call_count": 0-10,
  "first_seen": "...",
  "last_seen": "..."
}
```

**Interpretation:**
- **0-10 calls:** ✅ Normal - routine checks, no issues
- **10-50 calls:** ⚠️ Caution - some tables needed validation
- **50-100 calls:** ⚠️ Warning - multiple tables checked, investigate why
- **100+ calls:** 🚨 **ALERT** - Systematic stale state detected

**Action if >50 calls:**
1. Run audit script: `pipenv run python scripts/audit_table_row_counts.py`
2. Check ETL logs for "Falling back to full load" warnings
3. Identify which tables triggered HYBRID FIX
4. Investigate why stale state occurred (interrupted runs? config changes?)

---

### Check 2: ETL Log Warnings

**After each ETL run:**

```bash
# Check for HYBRID FIX trigger warnings
grep "Falling back to full load from replication" logs/etl_pipeline/etl_pipeline_run_*.log | tail -20
```

**Expected Output:**
- **First run after fix (Oct 21):** 10 warnings (recovering from stale state) ✅
- **Subsequent runs:** 0 warnings (all tables in sync) ✅

**Interpretation:**
- **0 warnings:** ✅ All tables loading correctly
- **1-2 warnings:** ⚠️ Monitor - may be new tables or minor issues
- **3+ warnings:** 🚨 **ALERT** - Recurring stale state problem

**Example Warning to Look For:**
```
WARNING - Incremental query returned 0 rows for medication, but analytics needs updating. Falling back to full load from replication.
INFO - HYBRID FIX: Successfully loaded 1085 rows for medication via full load fallback
```

---

### Check 3: Row Count Audit

**Run weekly (Mondays):**

```bash
cd etl_pipeline
pipenv run python scripts/audit_table_row_counts.py
```

**Expected Output:**
```
================================================================================
MISMATCHED TABLES: 0 tables with row count discrepancies
================================================================================

✅ No row count mismatches found! All tables are in sync.
```

**If mismatches found:**
1. Review the severity (CRITICAL/HIGH/MEDIUM)
2. Check if tables are known to have legitimate differences
3. For CRITICAL severity: Investigate immediately
4. Run targeted ETL for affected tables

---

## Weekly Validation (15 minutes)

### Check 1: dbt Test Suite

```bash
cd dbt_dental_models
dbt test
```

**Expected:**
```
Done. PASS=2100+ WARN=150-170 ERROR=0 SKIP=0
```

**Monitor for:**
- ❌ New ERROR tests (immediate investigation)
- ⚠️ Increase in WARN tests (trend analysis)
- ✅ PASS rate stable or improving

**Relationship test failures may indicate:**
- New stale state in ETL
- Source data issues
- Schema changes in source

---

### Check 2: Method Usage Trends

**Compare method_usage.json across weeks:**

```bash
# Save weekly snapshots
cp etl_pipeline/logs/method_usage.json etl_pipeline/logs/method_usage_$(date +%Y%m%d).json

# Compare trends
# Look for:
# - _check_analytics_needs_updating call count trending up (bad)
# - load_table_standard call count stable (expected)
# - bulk_insert_optimized call count proportional to data volume (expected)
```

**Healthy Trends:**
- `_check_analytics_needs_updating`: Stable at 0-10 per run
- `load_table_standard`: Stable (number of tables loading)
- `bulk_insert_optimized`: Proportional to data volume

**Unhealthy Trends:**
- `_check_analytics_needs_updating`: Increasing over time
- Indicates: Recurring stale state issues

---

## Monthly Review (30 minutes)

### Performance Analysis

**Review audit results history:**

```bash
# Compare audit results over time
ls -lh etl_pipeline/logs/table_audit_results_*.txt

# Check for patterns
grep "TOTAL MISSING ROWS" etl_pipeline/logs/table_audit_results_*.txt
```

**Expected:**
- First audit (Oct 21): "TOTAL MISSING ROWS: 484,831"
- Subsequent audits: "TOTAL MISSING ROWS: 0"

**Concerning patterns:**
- Missing rows increasing month-over-month
- Same tables repeatedly appearing in audit
- New tables appearing with large mismatches

---

### Method Tracking Analysis

**Key metrics to track:**

| Metric | What It Means | Healthy Range |
|--------|---------------|---------------|
| `_check_analytics_needs_updating` call rate | Stale state detection frequency | 0-10 per run |
| `bulk_insert_optimized` call count | Insert activity level | Stable or declining |
| `load_table_standard` vs `load_table_chunked` ratio | Table size distribution | ~95% standard |
| HYBRID FIX trigger rate | Self-healing frequency | 0% after recovery |

---

## Alert Thresholds

### Critical Alerts (Immediate Action Required)

🚨 **CRITICAL: Systematic Stale State Detected**
- `_check_analytics_needs_updating` calls > 100 per run
- Multiple HYBRID FIX warnings in logs
- Audit shows >10 tables with mismatches

**Action:**
1. Stop ETL pipeline
2. Run full audit: `pipenv run python scripts/audit_table_row_counts.py`
3. Review recent config changes to `tables.yml`
4. Check for interrupted ETL runs
5. Consider forcing full refresh for all affected tables

---

🚨 **CRITICAL: HYBRID FIX Not Recovering Data**
- HYBRID FIX warnings in logs
- But row count mismatches persist
- Same tables appearing in multiple audits

**Action:**
1. Check if HYBRID FIX code is functioning (may have been reverted)
2. Verify `_check_analytics_needs_updating()` returns correct values
3. Manual investigation of specific failing tables
4. May need to force full refresh with `force_full=True`

---

### Warning Alerts (Investigate Within 24 Hours)

⚠️ **WARNING: Increasing Stale State Trend**
- `_check_analytics_needs_updating` calls trending upward
- 1-3 HYBRID FIX warnings per run
- Same tables repeatedly needing recovery

**Action:**
1. Review ETL run logs for patterns
2. Check for infrastructure issues (interrupted runs)
3. Review incremental column configuration
4. May indicate need for full refresh cycle

---

⚠️ **WARNING: New Large Tables with Mismatches**
- Audit shows new tables with >1000 missing rows
- Tables that previously had no issues

**Action:**
1. Check when table was added to ETL
2. Verify initial load completed successfully
3. Review incremental column configuration for table
4. May need one-time full refresh

---

## Baseline Metrics (Post-Fix)

**Captured October 21, 2025 after successful recovery:**

### Method Call Counts (per ETL run)

| Method | Baseline Calls | Notes |
|--------|----------------|-------|
| `load_table` | ~34 | Total tables processed |
| `load_table_standard` | ~32 (94%) | Most common method |
| `load_table_streaming` | ~2 (6%) | Medium tables |
| `load_table_chunked` | ~4 (12%) | Large tables (overlap) |
| `_check_analytics_needs_updating` | 444 (recovery) → 0-10 (normal) | **KEY METRIC** |
| `bulk_insert_optimized` | ~150 | Batch inserts |
| `_filter_valid_incremental_columns` | ~34 | Once per table |
| `_build_enhanced_load_query` | ~34 | Once per table |
| `_update_load_status_hybrid` | ~20 | Tables with primary keys |

### Table Audit Results

**Baseline (Post-Recovery):**
```
Total tables checked: 420
Tables with mismatches: 0
TOTAL MISSING ROWS: 0
```

**Use this as reference for future audits.**

---

## Dashboard Queries

### Query 1: Real-Time Row Count Monitoring

```sql
-- Run in DBeaver against PostgreSQL analytics database
-- Shows current state of critical tables

SELECT 
    'document' as table_name, 
    COUNT(*) as current_count,
    255892 as expected_baseline,
    ROUND(100.0 * COUNT(*) / 255892, 2) as pct_complete
FROM opendental_analytics.raw.document
UNION ALL
SELECT 'appointment', COUNT(*), 78717, ROUND(100.0 * COUNT(*) / 78717, 2)
FROM opendental_analytics.raw.appointment
UNION ALL
SELECT 'claim', COUNT(*), 26773, ROUND(100.0 * COUNT(*) / 26773, 2)
FROM opendental_analytics.raw.claim
UNION ALL
SELECT 'medication', COUNT(*), 1092, ROUND(100.0 * COUNT(*) / 1092, 2)
FROM opendental_analytics.raw.medication
UNION ALL
SELECT 'inssub', COUNT(*), 13997, ROUND(100.0 * COUNT(*) / 13997, 2)
FROM opendental_analytics.raw.inssub
ORDER BY pct_complete;
```

**Expected:** All tables at ~100% (allowing for small growth)

**Alert if:** Any table drops below 95%

---

### Query 2: ETL Tracking Status Health

```sql
-- Check ETL tracking table for anomalies
SELECT 
    table_name,
    rows_loaded,
    load_status,
    last_primary_value,
    primary_column_name,
    _loaded_at,
    CASE 
        WHEN rows_loaded = 0 AND load_status = 'success' THEN '⚠️ SUSPICIOUS'
        WHEN load_status = 'failed' THEN '❌ FAILED'
        ELSE '✅ OK'
    END as health_status
FROM opendental_analytics.raw.etl_load_status
WHERE table_name IN ('document', 'appointment', 'claim', 'medication', 'inssub', 
                     'statement', 'insverify', 'insverifyhist', 'eobattach', 'ehrpatient')
ORDER BY health_status, table_name;
```

**Expected:** All tables show "✅ OK"

**Alert if:** Any table shows "⚠️ SUSPICIOUS" or "❌ FAILED"

---

## Automated Monitoring Setup (Future)

### Recommended Airflow DAG

```python
# Create: airflow/dags/monitor_etl_health.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def check_hybrid_fix_triggers():
    """Check method_usage.json for excessive HYBRID FIX triggers."""
    import json
    with open('etl_pipeline/logs/method_usage.json') as f:
        usage = json.load(f)
    
    check_analytics_calls = usage.get('PostgresLoader._check_analytics_needs_updating', {}).get('call_count', 0)
    
    if check_analytics_calls > 50:
        raise ValueError(f"ALERT: HYBRID FIX triggered {check_analytics_calls} times - investigate stale state!")

def run_table_audit():
    """Run table audit and alert on mismatches."""
    import subprocess
    result = subprocess.run(
        ['pipenv', 'run', 'python', 'scripts/audit_table_row_counts.py'],
        cwd='etl_pipeline',
        capture_output=True,
        text=True
    )
    
    if 'MISMATCHED TABLES: 0' not in result.stdout:
        raise ValueError("ALERT: Table row count mismatches detected!")

with DAG(
    'etl_health_monitoring',
    default_args={'start_date': datetime(2025, 10, 21)},
    schedule='0 1 * * *',  # Daily at 1 AM
    catchup=False
) as dag:
    
    check_hybrid_fix = PythonOperator(
        task_id='check_hybrid_fix_triggers',
        python_callable=check_hybrid_fix_triggers
    )
    
    audit_tables = PythonOperator(
        task_id='run_table_row_count_audit',
        python_callable=run_table_audit
    )
    
    dbt_tests = BashOperator(
        task_id='run_dbt_tests_critical',
        bash_command='cd dbt_dental_models && dbt test --select tag:foundation'
    )
    
    check_hybrid_fix >> audit_tables >> dbt_tests
```

---

## Monitoring Checklist

### Daily (Automated)

- [ ] Check `method_usage.json` for `_check_analytics_needs_updating` call count
  - **Threshold:** < 50 calls per run
  - **Alert if:** > 50 calls

- [ ] Scan ETL logs for "Falling back to full load" warnings
  - **Threshold:** 0 warnings
  - **Alert if:** > 2 warnings

- [ ] Verify ETL completion status
  - **Threshold:** All tables 'success'
  - **Alert if:** Any 'failed' status

### Weekly (Manual - 15 min)

- [ ] Run full table audit: `pipenv run python scripts/audit_table_row_counts.py`
  - **Threshold:** 0 mismatched tables
  - **Alert if:** Any tables >10% missing

- [ ] Run dbt tests: `dbt test`
  - **Threshold:** 0 errors
  - **Alert if:** New relationship test failures

- [ ] Review method_usage.json trends
  - Compare to baseline metrics
  - Look for unusual patterns

### Monthly (Manual - 30 min)

- [ ] Deep dive on method tracking data
  - Analyze call count trends
  - Identify performance bottlenecks
  - Validate method routing logic

- [ ] Review audit result history
  - Compare month-over-month
  - Track data growth rates
  - Identify slow-growing or shrinking tables

- [ ] Review ETL pipeline health
  - Average runtime trends
  - Error rate trends
  - Resource usage patterns

---

## Baseline Reference Values

### Normal Operating State (Post-Fix)

**Method Call Counts per Run:**
```
load_table: 30-40
load_table_standard: 28-35 (95%)
load_table_streaming: 2-4 (5%)
load_table_chunked: 4-8 (overlap with total)
_check_analytics_needs_updating: 0-10 ← KEY METRIC
bulk_insert_optimized: 100-200
_filter_valid_incremental_columns: 30-40
```

**Table Row Counts (Critical Tables):**
```
medication: 1,090-1,100 (slow growth)
document: 255,000+ (growing ~1000/week)
appointment: 78,000+ (growing ~500/week)
claim: 26,000+ (growing ~200/week)
inssub: 13,997+ (slow growth)
```

**Audit Results:**
```
Mismatched tables: 0
Total missing rows: 0
All tables: 100% loaded
```

---

## Troubleshooting Guide

### Scenario 1: HYBRID FIX Triggering Frequently

**Symptoms:**
- Multiple "Falling back to full load" warnings per run
- `_check_analytics_needs_updating` call count > 50

**Possible Causes:**
1. ETL runs being interrupted (infrastructure issues)
2. Config changes causing incremental logic to fail
3. Database connectivity issues during load
4. Insufficient transaction isolation

**Investigation Steps:**
1. Review ETL logs for error patterns
2. Check infrastructure monitoring (disk space, memory, network)
3. Review recent `tables.yml` configuration changes
4. Check PostgreSQL connection pool settings

**Fix:**
- Address infrastructure issues
- Stabilize ETL run environment
- Consider more conservative incremental strategies

---

### Scenario 2: Specific Table Always Has Mismatches

**Symptoms:**
- Same table appears in every audit
- HYBRID FIX recovers it each run
- But next audit shows mismatch again

**Possible Causes:**
1. Source table has continuous deletions/purges
2. Incremental column configuration incorrect
3. Time zone issues with timestamp columns
4. Data type conversion errors

**Investigation Steps:**
1. Check source table for delete operations
2. Verify incremental column reliability
3. Review data type conversions for table
4. Check if table has soft-delete pattern

**Fix:**
- Use full_table extraction strategy for table
- Update incremental column selection
- Add custom handling for table

---

### Scenario 3: New Tables Not Loading

**Symptoms:**
- New tables added to source
- Appear in analytics with 0 rows
- HYBRID FIX doesn't trigger

**Possible Causes:**
1. Table not in `tables.yml` configuration
2. Schema creation failed
3. Table filtered by extraction strategy

**Investigation Steps:**
1. Re-run schema analyzer: `python scripts/analyze_opendental_schema.py`
2. Check if table appears in `tables.yml`
3. Verify table exists in replication database
4. Check ETL logs for table-specific errors

**Fix:**
- Update `tables.yml` configuration
- Run initial full load for new table
- Verify schema compatibility

---

## Success Metrics

### Key Performance Indicators

**Data Completeness:**
- **Target:** 100% of replication data in analytics
- **Threshold:** > 99.5% acceptable
- **Alert:** < 99% requires investigation

**HYBRID FIX Efficiency:**
- **Target:** 0 triggers per run (perfect sync)
- **Threshold:** < 5 triggers acceptable (new tables, minor issues)
- **Alert:** > 10 triggers indicates problem

**Test Pass Rate:**
- **Target:** 0 relationship test errors
- **Threshold:** < 5 warnings acceptable
- **Alert:** Any errors require investigation

**ETL Runtime:**
- **Baseline:** 30-45 minutes for full pipeline
- **Threshold:** < 60 minutes acceptable
- **Alert:** > 90 minutes indicates performance degradation

---

## Reporting

### Daily Email Report Template

```
ETL Pipeline Health Report - [DATE]
=====================================

✅ Status: HEALTHY
⚠️  Status: WARNING  
❌ Status: CRITICAL

Key Metrics:
- Tables processed: 34
- HYBRID FIX triggers: 0
- Row count mismatches: 0
- dbt tests: PASS=2100+ WARN=160 ERROR=0

Action Required: None
```

### Weekly Summary Template

```
ETL Pipeline Weekly Summary - Week of [DATE]
=============================================

Data Completeness: 100%
Total rows loaded this week: [COUNT]
Tables with issues: 0

Method Usage Trends:
- _check_analytics_needs_updating: 0-10 calls/run ✅
- load_table_standard: 32 calls/run (stable)
- Average runtime: 35 minutes

Concerns: None
```

---

## Contact & Escalation

**For monitoring issues:**
1. Check this guide first
2. Review bug report: `etl_pipeline/docs/bug_report_incremental_loading_stale_state.md`
3. Review fix summary: `etl_pipeline/docs/bug_fix_summary_incremental_loading.md`

**Escalation path:**
- **Threshold exceeded:** Review troubleshooting guide
- **HYBRID FIX not working:** Code review required
- **Systematic failures:** Architecture review needed

---

**Document Owner:** Data Engineering Team  
**Last Updated:** October 21, 2025  
**Next Review:** November 21, 2025  
**Status:** Active Monitoring

