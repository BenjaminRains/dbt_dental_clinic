# AllergyDef Medication References - ETL Pipeline Bug (RESOLVED)

## Issue Summary
During data quality validation on October 20, 2025, we identified **5 apparent orphaned medication references** in `stg_opendental__allergydef` relationship tests. Investigation revealed this was **NOT a data quality issue** but rather a **critical ETL pipeline bug** affecting the medication table and 9 other critical tables.

**Identified Date:** October 20, 2025  
**Root Cause Identified:** October 20, 2025 (ETL incremental loading stale state bug)  
**Fixed Date:** October 21, 2025  
**Severity:** CRITICAL (was misclassified as WARN)  
**Status:** ✅ **RESOLVED** - All medications loaded, all tests passing

## Affected Allergy Definitions

| Allergy Def ID | Medication ID | Allergy Description | Status | Last Updated |
|----------------|---------------|---------------------|--------|--------------|
| 3 | 1515 | Aspirin | Active | 2025-09-23 |
| 4 | 137 | Codeine | Active | 2024-10-29 |
| 8 | 100 | Penicillin or Other Antibiotics | Active | 2024-10-29 |
| 67 | 630 | Morphine | Active | 2024-10-29 |
| 69 | 360 | ZPak (Azithromycin) | Active | 2024-10-29 |

## Actual Root Cause (ETL Pipeline Bug)

### What Actually Happened

**The medications were NEVER deleted.** Investigation revealed:

1. **Source Database (opendental):** All 1,090 medications exist ✅
2. **Replication Database:** All 1,090 medications exist ✅
3. **Analytics Database (raw.medication):** **ZERO medications loaded** ❌
4. **dbt Staging (stg_opendental__medication):** **ZERO medications available** ❌

**The Problem:** ETL pipeline incremental loading bug with stale state

### ETL Pipeline Bug Details

**Bug:** `PostgresLoader.load_table_standard()` lacked HYBRID FIX for stale state detection

**Sequence of Events:**
1. Oct 6, 2025: Initial load of 1,086 medications successful
2. Oct 6-11: Unknown event caused analytics table to only retain partial data
3. Oct 11: Tracking state recorded `last_primary_value = 1545` (highest medication ID in partial data)
4. Oct 19-20: ETL runs detected mismatch ("Replication has 1090 rows vs analytics 0-5 rows")
5. BUT: Incremental WHERE clause `MedicationNum > 1545` skipped all missing medications
6. Result: 0 rows loaded, stale state became permanent

**Impact:** Not just medication - **10 critical tables** affected with **484,831 total missing rows**

### Affected Tables (All Now Fixed)

| Table | Missing Rows | % Missing | Business Impact |
|-------|--------------|-----------|-----------------|
| document | 253,364 | 99.1% | Document analytics broken |
| appointment | 74,212 | 94.3% | Scheduling analytics broken |
| insverifyhist | 42,289 | 98.8% | Insurance verification history |
| statement | 27,416 | 99.5% | Patient billing analytics |
| insverify | 26,166 | 98.0% | Insurance verification |
| claim | 26,066 | 97.4% | Insurance claims analytics |
| eobattach | 18,957 | 99.5% | EOB documentation |
| inssub | 13,649 | 97.5% | Insurance subscriptions |
| **medication** | **1,090** | **100%** | **Medication catalog** |
| ehrpatient | 1,622 | 14.3% | EHR patient data |

## Resolution

### Fix Implemented (October 21, 2025)

**Code Changes:**
- Added HYBRID FIX to `load_table_standard()` (protects 95% of table loads)
- Added HYBRID FIX to `load_table_streaming()` (protects 5% of table loads)
- Added method tracking decorators for observability

**Data Recovery:**
- Truncated all 10 affected tables
- Cleared tracking state
- Re-ran ETL pipeline
- **Result:** All 484,831 missing rows successfully loaded ✅

**Verification:**
- All dbt tests now passing (PASS=13 WARN=0 ERROR=0)
- Medication table: 1,092 rows (100% complete)
- Allergy definitions correctly link to medications

### Technical Details

**Source System Validation (October 21, 2025):**
- ✅ `opendental.medication`: Medication IDs 100, 137, 360, 630, 1515 **ALL EXIST**
- ✅ `opendental_replication.medication`: All 1,090 medications **ALL EXIST**
- ✅ `opendental_analytics.raw.medication`: All 1,092 medications **NOW LOADED**
- ✅ `stg_opendental__allergydef`: All relationship tests **NOW PASSING**

### dbt Test Results (After Fix)
```bash
Done. PASS=13 WARN=0 ERROR=0 SKIP=0 TOTAL=13
```

**All tests now passing!** The medication_id relationship test that was failing is now successful.

**Location:** `models/staging/opendental/_stg_opendental__allergydef.yml`

---

## ETL Pipeline Bug Fix

### Code Changes Made

**File:** `etl_pipeline/etl_pipeline/loaders/postgres_loader.py`

1. **Added HYBRID FIX to `load_table_standard()`** (lines 1654-1714)
   - Detects when incremental query returns 0 rows
   - Checks if analytics row count < replication row count
   - Falls back to full load when mismatch detected
   - Protects 260 table loads (95% of all ETL operations)

2. **Added HYBRID FIX to `load_table_streaming()`** (lines 822-863)
   - Same logic adapted for streaming method
   - Protects 14 table loads (5% of all ETL operations)

3. **Added method tracking decorators**
   - 7 helper methods now tracked for observability
   - Enables monitoring of HYBRID FIX triggers

**Technical Documentation:**
- Bug Report: `etl_pipeline/docs/bug_report_incremental_loading_stale_state.md`
- Fix Summary: `etl_pipeline/docs/bug_fix_summary_incremental_loading.md`

### Data Recovery Process

**October 21, 2025 - Full Recovery Completed:**

```sql
-- 1. Cleared tracking state for all affected tables
DELETE FROM opendental_analytics.raw.etl_load_status 
WHERE table_name IN (
    'document', 'appointment', 'insverifyhist', 'statement',
    'insverify', 'claim', 'eobattach', 'inssub', 'medication', 'ehrpatient'
);

-- 2. Truncated affected tables
TRUNCATE TABLE opendental_analytics.raw.medication;
-- ... (all 10 tables)

-- 3. Re-ran ETL pipeline (automated via HYBRID FIX)
-- Result: All 484,831 missing rows loaded successfully
```

**Verification Results:**
- Medication table: 0 → 1,092 rows ✅
- All allergydef tests: PASS ✅
- All affected tables: 100% complete ✅

---

## Monitoring & Prevention

### Daily Monitoring (After Each ETL Run)

**1. Check method_usage.json for HYBRID FIX triggers:**
```bash
# Look for high call counts on _check_analytics_needs_updating
cat etl_pipeline/logs/method_usage.json | grep -A5 "_check_analytics_needs_updating"
```

**Expected:**
- Normal runs: 0-10 calls (checking tables as needed)
- High calls (>100): Indicates stale state detection (HYBRID FIX working)
- Should trend to 0 after initial recovery

**2. Check ETL logs for HYBRID FIX warnings:**
```bash
grep "Falling back to full load from replication" logs/etl_pipeline/etl_pipeline_run_*.log
```

**Expected:**
- First run after fix: 10 warnings (one per affected table)
- Subsequent runs: 0 warnings (all tables in sync)

**3. Run periodic table audit:**
```bash
cd etl_pipeline
pipenv run python scripts/audit_table_row_counts.py
```

**Expected:**
- "No row count mismatches found! All tables are in sync." ✅

### Weekly Validation

**Run comprehensive dbt tests:**
```bash
cd dbt_dental_models
dbt test
```

**Monitor for:**
- Relationship test failures (may indicate new stale state)
- Row count test failures
- Data freshness issues

### Monthly Review

**Check ETL performance metrics:**
- Review `logs/table_audit_results.txt` for trends
- Verify no tables developing row count drift
- Ensure HYBRID FIX call counts remain low (<10 per run)

---

## What We Learned

### Initial Misdiagnosis

**What we thought:** Medications were deleted from source database  
**Reality:** ETL pipeline bug prevented medications from loading to analytics

**Why the confusion:**
- dbt relationship tests failed (5 results)
- Appeared that medications 100, 137, 360, 630, 1515 didn't exist
- Actually: They existed in source but not in analytics

**Lesson:** Always verify at each layer (source → replication → analytics → dbt staging) before concluding root cause

### Impact of the Bug

**Not an isolated issue:**
- 10 critical tables affected
- 484,831 total missing rows
- 0.5-5% data completeness for critical analytics
- Appointment, claim, document, billing analytics all severely impacted

**Why it went undetected:**
- Most tables don't have relationship tests to other tables
- Medication was the "canary in the coal mine" due to allergydef foreign key
- Business users may have noticed incomplete dashboards but didn't report it

### Prevention Going Forward

**Now in Place:**
1. ✅ HYBRID FIX protects 100% of table loads
2. ✅ Method tracking provides observability
3. ✅ Audit script detects row count mismatches
4. ✅ Monitoring procedures documented

**Future Enhancements:**
- Add automated alerts for row count mismatches
- Implement pre-load validation in ETL
- Add relationship tests to more staging models
- Consider consolidating load methods (reduce from 5 to 2)

---

## Summary for Stakeholders

> **ISSUE RESOLVED:** What appeared to be 5 allergy definitions with deleted medication references was actually a critical ETL pipeline bug affecting 10 tables and 484,831 rows of data. The root cause was an incremental loading bug that caused stale state in the analytics database. 
>
> **FIX DEPLOYED:** October 21, 2025. All affected tables have been reloaded with complete data. The ETL pipeline now includes HYBRID FIX protection that automatically detects and recovers from stale state scenarios.
>
> **VERIFICATION:** All dbt tests passing. Medication table restored to 1,092 complete records. All allergy definitions properly linked to medications. No action required on allergy definitions.

---

## Related Documentation
- **Bug Report:** `etl_pipeline/docs/bug_report_incremental_loading_stale_state.md` (comprehensive analysis)
- **Fix Summary:** `etl_pipeline/docs/bug_fix_summary_incremental_loading.md` (implementation details)
- **Audit Script:** `etl_pipeline/scripts/audit_table_row_counts.py` (table comparison tool)
- **Staging Model:** `models/staging/opendental/stg_opendental__medication.sql`
- **Test Definition:** `models/staging/opendental/_stg_opendental__allergydef.yml`

---

**Document Status:** Updated to reflect actual root cause and resolution  
**Last Updated:** October 21, 2025  
**Original Issue Date:** October 20, 2025  
**Resolution Date:** October 21, 2025

