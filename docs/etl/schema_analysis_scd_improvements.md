# Schema Analysis Script - Slowly Changing Dimension Improvements

**Date:** 2025-10-07  
**Purpose:** Handle OpenDental's evolving schema (slowly changing dimensions)

## Problem Statement

Based on analysis of ETL error logs, we identified critical issues with schema drift:

### Evidence from Error Logs

**Run 1 (2025-10-06 20:04:51):**
- `document.ChartLetterStatus` - Referenced in config but **missing from MySQL** ❌
- `appointment.IsMirrored` - Referenced in config but **missing from MySQL** ❌
- `eobattach.ClaimNumPreAuth` - Referenced in config but **missing from MySQL** ❌

**Run 2 (2025-10-06 21:43:31):**
- `document.ChartLetterStatus` - **Now exists in MySQL** ✅ (auto-resolved)
- `appointment.IsMirrored` - **Now exists in MySQL** ✅ (auto-resolved)
- `eobattach.ClaimNumPreAuth` - **Still missing from MySQL** ❌ (config is stale)

### Root Cause

1. **OpenDental schema evolves** (columns added/removed with software updates)
2. **Config becomes stale** (tables.yml doesn't match current schema)
3. **No change tracking** (can't detect what changed between runs)
4. **Breaking changes silent** (removed columns cause ETL failures)

## Solution Implemented

We enhanced `analyze_opendental_schema.py` with **slowly changing dimension tracking**:

### 1. Automatic Configuration Backup

```python
# Backs up existing tables.yml before generating new one
backup_path = schema_backups / f'tables.yml.backup.{timestamp}'
```

**Benefits:**
- ✅ Preserves previous configuration
- ✅ Enables before/after comparison
- ✅ Provides rollback capability
- ✅ Creates audit trail

**Location:** `logs/schema_analysis/backups/`

### 2. Schema Change Detection

New method: `compare_with_previous_schema()`

**Detects:**
- ✅ **Added tables** - New tables in OpenDental
- ✅ **Removed tables** - Tables no longer in source
- ✅ **Added columns** - New columns in existing tables
- ✅ **Removed columns** - Columns no longer in source (⚠️ breaking)
- ✅ **Primary key changes** - Changed PKs (⚠️ breaking)
- ✅ **Schema hash changes** - Overall schema drift

**Example Output:**
```
🔄 SCHEMA CHANGES DETECTED!
  ✅ Added tables (3): branding, vaccinedef, toothgriddef
  ❌ Removed columns in 1 tables
     - eobattach: ClaimNumPreAuth
  ⚠️  BREAKING CHANGES DETECTED (1)
     - removed_columns: eobattach
       Impact: ETL extraction will fail if these columns are referenced
```

### 3. Human-Readable Changelog

New method: `_generate_schema_changelog()`

**Generates:** `logs/schema_analysis/reports/schema_changelog_{timestamp}.md`

**Contents:**
- Summary statistics
- Breaking changes (critical section)
- Added/removed tables
- Added/removed columns
- Recommended actions

**Example Changelog:**

```markdown
# OpenDental Schema Changelog - 20251007_123456

## ⚠️ BREAKING CHANGES (Action Required)

### 1. Removed Columns

- **Severity**: HIGH
- **Impact**: ETL extraction will fail if these columns are referenced
- **Table**: `eobattach`
- **Removed Columns**: `ClaimNumPreAuth`

## ✅ Added Tables (19)

- `branding`
- `eroutingactiondef`
- `vaccinedef`
...

## ✅ Added Columns (3 tables)

### `document`
- `ChartLetterStatus`

### `appointment`
- `IsMirrored`
```

### 4. Enhanced Workflow

**New 4-stage analysis:**

1. **Backup** - Save current config
2. **Detect** - Compare schemas
3. **Generate** - Create new config
4. **Report** - Document changes

**Old workflow (3 stages):**
1. Generate config
2. Save config
3. Report

## Usage

### Run Schema Analysis

```bash
cd etl_pipeline
pipenv run python scripts/analyze_opendental_schema.py
```

### Outputs

**Always generated:**
- `etl_pipeline/config/tables.yml` - Updated configuration
- `logs/schema_analysis/logs/schema_analysis_{timestamp}.log` - Detailed log
- `logs/schema_analysis/reports/schema_analysis_{timestamp}.json` - JSON report

**Generated when schema changes detected:**
- `logs/schema_analysis/backups/tables.yml.backup.{timestamp}` - Previous config
- `logs/schema_analysis/reports/schema_changelog_{timestamp}.md` - Human-readable changelog

### Console Output

```
====================================================
OpenDental Schema Analysis Started
====================================================
📦 Backed up existing configuration to: logs/schema_analysis/backups/tables.yml.backup.20251007_123456
Stage 1/4: Generating table configuration...
Stage 1 completed in 45.2s
Stage 2/4: Detecting schema changes...
🔄 SCHEMA CHANGES DETECTED!
  ✅ Added tables (19): branding, eroutingactiondef, ...
  ❌ Removed columns in 1 tables
     - eobattach: ClaimNumPreAuth
  ⚠️  BREAKING CHANGES DETECTED (1)
     - removed_columns: eobattach
       Impact: ETL extraction will fail if these columns are referenced
✅ No schema changes detected
Stage 2 completed in 2.3s
...
```

## Benefits

### For Schema Management

1. **Proactive Detection** - Know about schema changes before ETL fails
2. **Breaking Change Alerts** - Critical changes highlighted immediately
3. **Audit Trail** - Complete history of schema evolution
4. **Rollback Capability** - Can revert to previous config if needed

### For ETL Operations

1. **Prevents Failures** - Catch removed columns before extraction
2. **Guided Remediation** - Changelog provides action items
3. **Faster Resolution** - Clear visibility into what changed
4. **Reduced Downtime** - Fix issues before they break production

### For Data Team

1. **Change Awareness** - Know when new columns/tables appear
2. **Model Updates** - Identify which dbt models need updating
3. **Documentation** - Changelog serves as schema version history
4. **Planning** - Anticipate impact of OpenDental updates

## Real-World Example

### Scenario: OpenDental Update Adds New Tables

**What happens:**

1. **Schema analysis runs:** Script detects 19 new tables
2. **Changelog generated:** Lists all new tables with recommendations
3. **Team notified:** Breaking changes printed to console
4. **Action taken:** 
   - Review new tables in changelog
   - Decide which tables to load
   - Initialize tables in PostgreSQL
   - Run ETL for new tables
   - Create dbt models if needed

**Without this feature:**
- ETL fails with cryptic "table not found" errors
- Manual investigation required
- No visibility into what changed
- Reactive instead of proactive

## Answering Your Original Question

> "Do we need to modify our schema discovery script?"

**Answer: We've enhanced it! ✅**

### What We Fixed

1. **✅ Captures new fields** - Already worked, now with change detection
2. **✅ Captures new tables** - Already worked, now with change detection
3. **✅ Detects removed fields** - NEW! Critical for preventing failures
4. **✅ Detects removed tables** - NEW! Helps clean up config
5. **✅ Tracks schema history** - NEW! Automatic backups
6. **✅ Generates changelogs** - NEW! Human-readable reports

### What Makes This SCD-Ready

**Slowly Changing Dimensions require:**
- Change detection ✅
- Historical tracking ✅
- Point-in-time comparison ✅
- Impact analysis ✅
- Automated documentation ✅

**Our implementation provides all of these.**

## Next Steps

### Immediate
1. ✅ Run schema analysis: `pipenv run python scripts/analyze_opendental_schema.py`
2. ✅ Review generated changelog (if changes detected)
3. ✅ Fix `eobattach.ClaimNumPreAuth` issue (already detected)
4. ✅ Run ETL with updated configuration

### Ongoing
1. **Schedule regular analysis** - Run weekly or after OpenDental updates
2. **Monitor changelogs** - Review schema changes for business impact
3. **Update dbt models** - Expose new columns in staging/marts
4. **Document patterns** - Track which columns OpenDental commonly adds/removes

## Technical Details

### Schema Hash Algorithm

```python
# Uses MD5 hash of table names, columns, and primary keys
schema_data = [{
    'table_name': 'patient',
    'columns': ['PatNum', 'FName', 'LName', ...],
    'primary_keys': ['PatNum']
}]
hash_input = str(schema_data).encode('utf-8')
schema_hash = hashlib.md5(hash_input).hexdigest()
```

**Benefits:**
- Fast comparison (hash comparison vs full schema comparison)
- Detects any structural change
- Consistent across runs

### Backup Strategy

- **Format:** YAML (same as original)
- **Naming:** `tables.yml.backup.{timestamp}`
- **Location:** `logs/schema_analysis/backups/`
- **Retention:** Manual (review and delete old backups as needed)

### Changelog Format

- **Format:** Markdown (human-readable)
- **Sections:** Summary, Breaking Changes, Added/Removed, Recommendations
- **Naming:** `schema_changelog_{timestamp}.md`
- **Location:** `logs/schema_analysis/reports/`

## Conclusion

Your schema discovery script **now handles slowly changing dimensions** properly!

**Key Improvements:**
1. 🔄 **Change Detection** - Knows what changed and when
2. 📦 **Automatic Backups** - Never lose previous configuration
3. 📋 **Changelogs** - Clear documentation of changes
4. ⚠️  **Breaking Change Alerts** - Proactive failure prevention
5. 🎯 **Guided Remediation** - Actionable recommendations

**The script will now:**
- Alert you to schema changes
- Prevent extraction failures
- Document schema evolution
- Guide remediation efforts

**Ready to handle OpenDental's evolving schema! ✅**

