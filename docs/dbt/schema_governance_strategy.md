# Schema Governance Strategy: Managing SCDs Across ETL and DBT

## Executive Summary

**Problem**: When source database schemas evolve (Slowly Changing Dimensions), we must manually update multiple layers:
- ETL configuration (`tables.yml`)
- DBT source definitions (`_sources/*.yml`) 
- DBT staging models (`*.sql`)
- DBT model documentation (`_stg_*.yml`)
- Downstream DBT models (intermediate/marts)

**Impact**: Schema drift causes pipeline failures, data quality issues, and significant manual maintenance overhead.

**Solution**: Implement semi-automated schema governance with intelligent column detection, validation, and guided updates.

---

## Table of Contents
1. [Understanding Slowly Changing Dimensions (SCDs)](#understanding-slowly-changing-dimensions-scds)
2. [Current State](#current-state)
3. [The Problem](#the-problem)
4. [Schema Change Lifecycle](#schema-change-lifecycle)
5. [Strategy Options](#strategy-options)
6. [Recommended Approach](#recommended-approach)
7. [Implementation Roadmap](#implementation-roadmap)
8. [Best Practices](#best-practices)
9. [Troubleshooting Guide](#troubleshooting-guide)

---

## Understanding Slowly Changing Dimensions (SCDs)

### What are Slowly Changing Dimensions?

**Slowly Changing Dimensions (SCDs)** are dimensions in a data warehouse that change over time, but at a slow, irregular pace rather than on a regular schedule. The term describes how we handle changes to dimensional data (like customer addresses, product prices, provider information) in our analytics database.

**Examples from OpenDental**:
- A patient moves and their address changes
- A provider's phone number is updated
- An insurance plan's fee schedule changes
- A clinic's operating hours are modified

The question is: **How do we track these changes?** Do we:
- Overwrite the old value and lose history?
- Keep both old and new values with effective dates?
- Store only the original value and track changes separately?

---

### SCD Type 1: Overwrite (No History)

**Strategy**: Simply overwrite the old value with the new value. No history is preserved.

**Use Case**: When historical values don't matter for analysis, or when the old value was simply wrong (data correction).

**Example - Provider Phone Number**:
```sql
-- BEFORE UPDATE
ProvNum | LName    | Phone
--------|----------|------------
123     | Smith    | 555-0100

-- AFTER UPDATE (phone changed)
ProvNum | LName    | Phone
--------|----------|------------
123     | Smith    | 555-0200  -- Old number is gone forever
```

**ETL Strategy**: 
- Use **full refresh** or **standard upsert**
- Latest data wins
- No special tracking columns needed

**DBT Handling**:
```sql
-- Staging model - simple overwrite
select
    provider_id,
    last_name,
    phone  -- Always shows current value
from {{ source('opendental', 'provider') }}
```

**Pros**:
- ✅ Simple to implement
- ✅ Minimal storage requirements
- ✅ Easy to understand and query

**Cons**:
- ❌ Complete loss of historical data
- ❌ Can't answer "what was the phone number on X date?"
- ❌ Can't audit changes over time

**OpenDental Tables Using SCD Type 1**:
- `provider` - Provider demographics (when history doesn't matter)
- `clinic` - Clinic contact info (current info only)
- `definition` - System definitions (current values)
- `carrier` - Insurance carrier info (current only)
- `disease` - Disease definitions (current values only)

---

### SCD Type 2: Add New Row with Effective Dates (Full History)

**Strategy**: When a value changes, add a new row with effective date ranges. Keep all historical versions.

**Use Case**: When you need complete historical accuracy for reporting, auditing, or point-in-time analysis.

**Example - Patient Address**:
```sql
-- BEFORE UPDATE
PatNum | Address          | EffectiveDate | EndDate    | IsCurrent
-------|------------------|---------------|------------|----------
456    | 123 Oak St       | 2020-01-01    | 9999-12-31 | TRUE

-- AFTER UPDATE (address changed on 2024-03-15)
PatNum | Address          | EffectiveDate | EndDate    | IsCurrent
-------|------------------|---------------|------------|----------
456    | 123 Oak St       | 2020-01-01    | 2024-03-14 | FALSE  -- Old address
456    | 456 Maple Ave    | 2024-03-15    | 9999-12-31 | TRUE   -- New address
```

**ETL Strategy**:
- Detect changes by comparing current data to previous load
- When change detected:
  1. Update previous row: Set `EndDate` = current_date - 1, `IsCurrent` = FALSE
  2. Insert new row: Set `EffectiveDate` = current_date, `EndDate` = '9999-12-31', `IsCurrent` = TRUE
- Requires tracking columns:
  - `EffectiveDate` or `ValidFrom`
  - `EndDate` or `ValidTo`
  - `IsCurrent` or `IsActive` flag
  - Often: `RowHash` for change detection

**DBT Handling**:
```sql
-- Staging model - include SCD columns
select
    patient_id,
    address,
    effective_date,
    end_date,
    is_current,
    _row_hash  -- For change detection
from {{ source('opendental', 'patient') }}

-- Intermediate model - current records only
select *
from {{ ref('stg_opendental__patient') }}
where is_current = true

-- Analytics query - point-in-time lookup
select *
from {{ ref('stg_opendental__patient') }}
where '2023-06-15' between effective_date and end_date
```

**Pros**:
- ✅ Complete historical accuracy
- ✅ Can answer "what was X on date Y?"
- ✅ Full audit trail of changes
- ✅ Supports as-of reporting

**Cons**:
- ❌ More complex to implement
- ❌ Larger storage requirements (multiple rows per entity)
- ❌ More complex queries (need to filter for current or specific date)
- ❌ Requires robust change detection logic

**OpenDental Tables Using SCD Type 2**:
- `patient` - Patient demographics (need history for auditing)
- `insplan` - Insurance plans (rates change, need history)
- `fee` - Procedure fees (need historical pricing)
- `feesched` - Fee schedules (track changes over time)

**Change Detection Pattern**:
The ETL pipeline detects SCD Type 2 candidates by looking for these column patterns:
```python
# In analyze_opendental_schema.py
SCD_TYPE_2_PATTERNS = [
    'DateTStamp',      # Timestamp of last modification
    'SecDateTEdit',    # Security timestamp
    'SecDateEntry',    # Entry timestamp
]

# If table has timestamp column indicating changes, likely SCD Type 2
if any(col in table_columns for col in SCD_TYPE_2_PATTERNS):
    scd_type = 2
```

---

### SCD Type 3: Add New Column (Limited History)

**Strategy**: Keep both current and previous value in separate columns. Only track one level of history.

**Use Case**: When you need to compare "before" vs "after" but don't need full history. Common for tracking recent changes or migrations.

**Example - Patient Primary Insurance**:
```sql
-- BEFORE UPDATE
PatNum | CurrentPlan | PreviousPlan | PlanChangeDate
-------|-------------|--------------|---------------
789    | Plan_A      | NULL         | NULL

-- AFTER UPDATE (switched to Plan_B on 2024-05-01)
PatNum | CurrentPlan | PreviousPlan | PlanChangeDate
-------|-------------|--------------|---------------
789    | Plan_B      | Plan_A       | 2024-05-01

-- AFTER ANOTHER UPDATE (switched to Plan_C on 2024-08-01)
PatNum | CurrentPlan | PreviousPlan | PlanChangeDate
-------|-------------|--------------|---------------
789    | Plan_C      | Plan_B       | 2024-08-01  -- Lost Plan_A history!
```

**ETL Strategy**:
- When change detected:
  1. Move current value to "previous" column
  2. Update current column with new value
  3. Update change timestamp
- Requires columns like:
  - `Current{Field}` and `Previous{Field}`
  - `{Field}ChangeDate`
  - Or: `{Field}` and `{Field}Old`

**DBT Handling**:
```sql
-- Staging model - expose both values
select
    patient_id,
    current_insurance_plan_id,
    previous_insurance_plan_id,
    plan_change_date,
    
    -- Derived logic
    case 
        when previous_insurance_plan_id is not null 
        then true 
        else false 
    end as has_changed_plan
    
from {{ source('opendental', 'patient') }}

-- Analytics - track recent plan switchers
select
    patient_id,
    current_insurance_plan_id,
    previous_insurance_plan_id,
    plan_change_date
from {{ ref('stg_opendental__patient') }}
where plan_change_date >= current_date - interval '90 days'
```

**Pros**:
- ✅ Simple to implement (no multi-row logic)
- ✅ Efficient queries (single row per entity)
- ✅ Tracks immediate before/after comparison
- ✅ Minimal storage overhead

**Cons**:
- ❌ Only one level of history (loses older changes)
- ❌ Can't do point-in-time reporting beyond previous value
- ❌ Limited audit trail
- ❌ Less common (most systems use Type 1 or Type 2)

**OpenDental Tables Using SCD Type 3**:
- **Rare in OpenDental** - Most tables use Type 1 or Type 2
- Possible use case: Tracking recent status changes with `Status` and `PreviousStatus` columns

---

### SCD Type Comparison Table

| Aspect | Type 1 | Type 2 | Type 3 |
|--------|--------|--------|--------|
| **History Preserved** | None | Full | Limited (1 previous) |
| **Storage** | Minimal | High (multiple rows) | Low (extra columns) |
| **Complexity** | Simple | Complex | Medium |
| **Query Complexity** | Simple | Medium (date filtering) | Simple |
| **Point-in-time Reporting** | No | Yes | Limited (previous only) |
| **Audit Trail** | No | Complete | Partial |
| **Common Use** | Very common | Common | Rare |

---

### How the Schema Analyzer Detects SCD Type

The ETL pipeline's `analyze_opendental_schema.py` script automatically detects SCD type based on column patterns:

```python
def determine_scd_type(table_name: str, columns: List[str]) -> int:
    """
    Determine the appropriate SCD type for a table based on its columns.
    
    Returns:
        1 = SCD Type 1 (overwrite, no history)
        2 = SCD Type 2 (versioned rows with effective dates)
        3 = SCD Type 3 (current + previous columns) - rare
    """
    
    # SCD Type 2 Indicators
    type_2_patterns = [
        'DateTStamp',      # OpenDental modification timestamp
        'SecDateTEdit',    # Security edit date
        'SecDateEntry',    # Security entry date
    ]
    
    # Check for Type 2 timestamp columns
    has_type_2_indicator = any(col in columns for col in type_2_patterns)
    
    # SCD Type 3 Indicators (rare)
    type_3_patterns = [
        ('Current', 'Previous'),  # Paired current/previous columns
        ('New', 'Old'),           # Paired new/old columns
    ]
    
    # Check for Type 3 paired columns
    for current_pattern, previous_pattern in type_3_patterns:
        current_cols = [c for c in columns if current_pattern in c]
        previous_cols = [c for c in columns if previous_pattern in c]
        if current_cols and previous_cols:
            return 3
    
    # Default decision
    if has_type_2_indicator:
        return 2  # Has change tracking → SCD Type 2
    else:
        return 1  # No change tracking → SCD Type 1
```

**Example Detection Results**:

```yaml
# tables.yml excerpt showing detected SCD types

patient:
  scd_type: 2  # ← Has DateTStamp column
  primary_key: PatNum
  incremental_column: DateTStamp
  columns:
    - PatNum
    - LName
    - FName
    - Address
    - DateTStamp  # ← Triggers SCD Type 2 detection

provider:
  scd_type: 1  # ← No timestamp tracking
  primary_key: ProvNum
  incremental_column: null
  full_refresh: true
  columns:
    - ProvNum
    - LName
    - FName
    - Phone
```

---

### SCD Type Impact on ETL and DBT

#### SCD Type 1 - ETL Implementation
```python
# Simple upsert - overwrites existing data
def load_scd_type_1(table_name):
    # Extract current data from source
    source_data = extract_from_mysql(table_name)
    
    # Load to PostgreSQL (upsert)
    postgres_loader.upsert(
        table=table_name,
        data=source_data,
        conflict_action='UPDATE'  # Overwrites on conflict
    )
```

#### SCD Type 2 - ETL Implementation
```python
# Track historical changes with versioning
def load_scd_type_2(table_name):
    # Extract current data from source
    source_data = extract_from_mysql(table_name)
    
    # Get existing data from target
    existing_data = postgres_loader.query(
        f"SELECT * FROM raw.{table_name} WHERE is_current = true"
    )
    
    # Detect changes (compare row hashes)
    for row in source_data:
        row_hash = calculate_hash(row)
        existing_row = find_existing_row(existing_data, row['primary_key'])
        
        if existing_row and existing_row['_row_hash'] != row_hash:
            # Value changed - implement SCD Type 2
            
            # Step 1: Close out old row
            postgres_loader.update(
                table=table_name,
                where={"primary_key": row['primary_key'], "is_current": True},
                values={
                    "end_date": current_date - 1,
                    "is_current": False
                }
            )
            
            # Step 2: Insert new row with current version
            postgres_loader.insert(
                table=table_name,
                values={
                    **row,
                    "effective_date": current_date,
                    "end_date": '9999-12-31',
                    "is_current": True,
                    "_row_hash": row_hash
                }
            )
        elif not existing_row:
            # New row - insert with SCD metadata
            postgres_loader.insert(
                table=table_name,
                values={
                    **row,
                    "effective_date": current_date,
                    "end_date": '9999-12-31',
                    "is_current": True,
                    "_row_hash": row_hash
                }
            )
```

#### DBT Implications

**For SCD Type 1 tables**:
```sql
-- Simple staging model
{{ config(materialized='view') }}

select
    patient_id,
    last_name,
    first_name,
    phone  -- Always current value
from {{ source('opendental', 'provider') }}
```

**For SCD Type 2 tables**:
```sql
-- Staging model includes SCD columns
{{ config(
    materialized='incremental',
    unique_key=['patient_id', 'effective_date']
) }}

select
    patient_id,
    last_name,
    first_name,
    address,
    
    -- SCD Type 2 metadata
    effective_date,
    end_date,
    is_current,
    _row_hash
    
from {{ source('opendental', 'patient') }}

-- For intermediate/mart models, often filter to current only
-- unless doing historical analysis
```

**Intermediate model for current records**:
```sql
-- int_current_patients.sql
select *
from {{ ref('stg_opendental__patient') }}
where is_current = true
```

**Analytics query for point-in-time**:
```sql
-- "What was the patient's address on June 1, 2023?"
select
    patient_id,
    address
from {{ ref('stg_opendental__patient') }}
where '2023-06-01' between effective_date and end_date
```

---

### Why SCD Type Detection Matters

1. **Storage Planning**: SCD Type 2 tables grow faster (multiple rows per entity)
2. **ETL Strategy**: Type 2 requires change detection and versioning logic
3. **Query Performance**: Type 2 requires date filtering (indexed properly?)
4. **Business Logic**: Type 2 enables historical analysis, Type 1 doesn't
5. **DBT Modeling**: Affects how you build intermediate and mart models

**Example - Patient Table**:
- **Type 1**: 10,000 patients = 10,000 rows
- **Type 2**: 10,000 patients with 3 address changes each = 40,000 rows

---

## Current State

### ETL Pipeline Schema Management
**Location**: `etl_pipeline/etl_pipeline/config/tables.yml`

**Current Capabilities**:
- ✅ Automated schema analysis from MySQL source
- ✅ Detects new tables and columns
- ✅ Identifies SCD type (1, 2, or 3) based on column patterns
- ✅ Updates ETL configuration automatically
- ✅ Handles data type inference and transformation rules

**Command**: `python scripts/analyze_opendental_schema.py`

**What it does**:
1. Connects to MySQL OpenDental database
2. Queries `INFORMATION_SCHEMA` for table/column metadata
3. Analyzes column patterns (timestamps, booleans, IDs)
4. Determines incremental/full refresh strategies
5. Updates `tables.yml` with new schema
6. Backs up previous configuration

### DBT Model Schema Management
**Location**: `dbt_dental_models/models/`

**Current State**:
- ❌ **Manual updates required** for all schema changes
- ❌ No automated detection of source schema drift
- ❌ No validation between ETL and DBT schemas
- ❌ Documentation often lags behind code changes

**Affected Files Per Table**:
1. **Source Definition**: `staging/opendental/_sources/*.yml`
   - Column list and documentation
   - Tests (unique, not_null, relationships)
   - Freshness checks

2. **Staging Model**: `staging/opendental/stg_opendental__*.sql`
   - Column transformations
   - Data type conversions
   - Naming standardization (PascalCase → snake_case)

3. **Model Documentation**: `staging/opendental/_stg_opendental__*.yml`
   - Column descriptions
   - Business rules
   - Data quality tests
   - Known issues and metadata

4. **Downstream Models**: `intermediate/`, `marts/`
   - May need new columns added
   - May need new joins for foreign keys
   - May need updated business logic

---

## The Problem

### Real-World Example: `eobattach` Table

**Scenario**: OpenDental adds new column `ClaimNumPreAuth` to link EOB attachments to pre-authorization claims.

**Impact Cascade**:
1. ❌ ETL extraction fails: "Unknown column 'ClaimNumPreAuth' in 'field list'"
2. ⚠️ Schema analyzer adds column to `tables.yml` (automated ✅)
3. ❌ DBT source definition missing column → source validation fails
4. ❌ DBT staging model missing transformation → incomplete data
5. ❌ DBT documentation missing column → poor data quality
6. ⚠️ Downstream models may need foreign key joins → business logic incomplete

**Manual Work Required** (current process):
- Run schema analyzer (~5 min)
- Update DBT source YAML (~5 min)
- Update DBT staging SQL (~10 min)
- Update DBT model docs (~10 min)
- Check downstream models (~15 min)
- Test and validate (~15 min)
- **Total: ~1 hour per table**

**With 436 tables**, schema changes are frequent and costly.

### Common Schema Change Patterns

| Change Type | Frequency | Complexity | Current Pain Level |
|-------------|-----------|------------|-------------------|
| New column (non-FK) | High | Low | Medium - Manual updates |
| New foreign key | Medium | Medium | High - Need relationship tests |
| Column rename | Low | High | Critical - Breaks all layers |
| Column deletion | Low | High | Critical - Need safe removal |
| Data type change | Low | Medium | High - Type conversion needed |
| New table | Medium | High | Very High - Full model creation |

---

## Schema Change Lifecycle

### Phase 1: Source Schema Evolution (OpenDental)
```
OpenDental Update → MySQL Schema Changes
```
**Detection**: Schema analyzer queries MySQL `INFORMATION_SCHEMA`

### Phase 2: ETL Configuration Update
```
MySQL Schema → tables.yml Update → Backup Created
```
**Tool**: `analyze_opendental_schema.py` (automated ✅)

### Phase 3: ETL Pipeline Execution
```
tables.yml → Extract Data → Load to PostgreSQL raw schema
```
**Tool**: `etl-run` command

### Phase 4: DBT Source Sync (MANUAL ❌)
```
PostgreSQL raw schema → DBT source definitions
```
**Current Gap**: No automated sync between ETL output and DBT sources

### Phase 5: DBT Model Update (MANUAL ❌)
```
DBT sources → Staging models → Model docs → Downstream models
```
**Current Gap**: All updates are manual

### Phase 6: Validation & Testing
```
dbt parse → dbt compile → dbt test → dbt run
```
**Tool**: DBT CLI (manual execution)

---

## Strategy Options

### Option 1: Manual Templates + Warnings ⭐
**Effort**: Low (1-2 days)  
**Automation**: Minimal  
**Control**: High  

**How it Works**:
1. Schema analyzer detects ETL → DBT drift
2. Generates **warning report** with:
   - List of tables with missing columns
   - Template snippets for each layer
   - Step-by-step manual instructions
3. Developer manually applies changes
4. No automatic file modification

**Pros**:
- ✅ Quick to implement
- ✅ Developer maintains full control
- ✅ Good for complex transformations
- ✅ Low risk of incorrect automation

**Cons**:
- ❌ Still fully manual updates
- ❌ Easy to miss layers or make mistakes
- ❌ No validation that changes were applied correctly
- ❌ Doesn't scale well with many tables

**Best For**: 
- Infrequent schema changes
- Complex domain logic requiring human judgment
- Teams uncomfortable with automated code changes

---

### Option 2: Semi-Automated with Review ⭐⭐⭐ (RECOMMENDED)
**Effort**: Medium (3-5 days)  
**Automation**: High  
**Control**: Medium-High  

**How it Works**:
1. **Detection Phase**: 
   - Compare `tables.yml` → DBT source definitions
   - Identify missing columns, type changes, new tables
   
2. **Analysis Phase**:
   - Determine column purpose using pattern matching:
     - `*Num` → Foreign key (transform to `*_id`)
     - `Is*`, `Has*` → Boolean (apply boolean conversion)
     - `Date*`, `*Date` → Datetime (apply date cleaning)
     - Others → String/numeric passthrough
   
3. **Generation Phase**:
   - Auto-generate required changes for each layer
   - Create diff/patch files for review
   - Generate documentation stubs with intelligent descriptions
   
4. **Review Phase**:
   - Present changes to developer in organized format
   - Developer reviews, edits, approves each change
   
5. **Application Phase**:
   - Apply approved changes atomically
   - Run `dbt parse` to validate
   - Create backup before changes

**Implementation Components**:

```python
# New functions in analyze_opendental_schema.py

def compare_etl_to_dbt_sources(tables_yml_path, dbt_sources_dir):
    """
    Compare ETL tables.yml to DBT source definitions.
    Returns: Dict of {table_name: [missing_columns]}
    """
    pass

def detect_column_purpose(column_name, mysql_type, table_context):
    """
    Intelligently determine column purpose and transformation.
    Returns: {
        'type': 'foreign_key' | 'boolean' | 'datetime' | 'string' | 'numeric',
        'target_name': 'snake_case_name',
        'transformation': 'macro_call or direct mapping',
        'test_suggestions': [list of suggested dbt tests]
    }
    """
    # Pattern matching logic:
    # - Foreign keys: *Num, *ID (excluding primary key)
    # - Booleans: Is*, Has*, Can*, Should*
    # - Dates: Date*, *Date, *Time, *Stamp
    # - Status codes: *Status, *Type (with enum detection)
    pass

def generate_dbt_source_update(table_name, missing_columns):
    """
    Generate YAML snippet for source definition.
    Returns: YAML string with proper indentation
    """
    pass

def generate_staging_model_update(table_name, missing_columns):
    """
    Generate SQL transformation code.
    Returns: SQL snippet with proper macro usage
    """
    pass

def generate_schema_doc_update(table_name, missing_columns):
    """
    Generate YAML documentation with intelligent descriptions.
    Returns: YAML string with tests and descriptions
    """
    pass

def create_update_package(table_name, changes):
    """
    Create organized update package in docs/schema_updates/.
    Generates:
    - {table}_source_updates.yml
    - {table}_staging_updates.sql  
    - {table}_schema_docs.yml
    - {table}_CHANGES.md (summary report)
    """
    pass

def apply_updates_interactive(update_package):
    """
    Interactive CLI to review and apply updates.
    Shows diffs, allows editing, applies changes.
    """
    pass
```

**Example Output**:
```bash
$ python scripts/analyze_opendental_schema.py --check-dbt

🔍 Analyzing DBT schema drift...

Found 3 tables with schema changes:
  ✓ eobattach (1 new column)
  ✓ appointment (1 new column) 
  ✓ document (1 new column)

Generating update packages...
  ✓ docs/schema_updates/eobattach_updates/
    - source_updates.yml
    - staging_updates.sql
    - schema_docs.yml
    - CHANGES.md

📋 Review updates with:
  python scripts/apply_dbt_schema_updates.py --review

🚀 Apply updates with:
  python scripts/apply_dbt_schema_updates.py --apply
```

**Pros**:
- ✅ Automated detection and generation
- ✅ Developer reviews before applying (safety)
- ✅ Ensures all layers are updated consistently
- ✅ Handles 80% of common cases automatically
- ✅ Generates intelligent documentation stubs
- ✅ Validates changes with `dbt parse`

**Cons**:
- ⚠️ Complex transformations still need manual editing
- ⚠️ Requires moderate development effort (3-5 days)
- ⚠️ Pattern matching may miss edge cases

**Best For**:
- Regular schema changes (weekly/monthly)
- Medium-to-large table counts (100+ tables)
- Teams comfortable with semi-automated workflows
- **This is our recommended approach**

---

### Option 3: Fully Automated (AI-Assisted) ⭐⭐
**Effort**: High (2-3 weeks)  
**Automation**: Very High  
**Control**: Low  

**How it Works**:
1. Schema analyzer detects changes
2. **AI/LLM determines column purpose** using:
   - Column name semantics
   - Table context and relationships
   - Historical patterns in codebase
   - OpenDental documentation
3. **Auto-applies** updates across all layers
4. **Auto-validates** with `dbt compile` and `dbt test`
5. Creates **auto-commit** or **pull request** with changes
6. Notifies team for post-merge review

**Additional Intelligence**:
- Detects foreign key targets by analyzing table relationships
- Generates meaningful column descriptions using AI
- Updates downstream models that use `select *` patterns
- Detects breaking changes and suggests migration paths
- Auto-generates dbt tests based on data profiling

**Pros**:
- ✅ Fully automated end-to-end
- ✅ Handles complex scenarios intelligently
- ✅ Zero manual work for standard cases
- ✅ Scales to any number of tables

**Cons**:
- ❌ Complex to implement (2-3 weeks effort)
- ❌ Requires LLM integration (cost, latency)
- ❌ May make incorrect assumptions
- ❌ Harder to debug when wrong
- ❌ Less developer control and understanding

**Best For**:
- Very large table counts (500+ tables)
- High-frequency schema changes (daily)
- Mature MLOps teams with AI infrastructure
- **Not recommended for current project maturity**

---

## Recommended Approach

### Phase 1: Implement Semi-Automated Detection (Week 1)

**Goal**: Detect ETL → DBT schema drift automatically

**Tasks**:
1. Extend `analyze_opendental_schema.py`:
   ```python
   # Add new command-line flag
   --check-dbt  # Compare tables.yml to DBT sources
   ```

2. Implement drift detection:
   - Parse `tables.yml` to extract column lists
   - Parse DBT `_sources/*.yml` to extract DBT columns
   - Compare and identify differences:
     - Columns in ETL but not in DBT (new columns)
     - Columns in DBT but not in ETL (removed columns)
     - Type mismatches (data type changes)

3. Generate drift report:
   ```
   docs/schema_drift/drift_report_YYYYMMDD.md
   ```

**Deliverable**: Automated schema drift detection

---

### Phase 2: Implement Intelligent Column Analysis (Week 2)

**Goal**: Automatically determine column purpose and transformation

**Tasks**:
1. Create pattern-matching rules:
   ```python
   FOREIGN_KEY_PATTERNS = [
       r'.*Num$',  # e.g., PatNum, ClaimNum
       r'.*ID$',   # e.g., PatientID
   ]
   
   BOOLEAN_PATTERNS = [
       r'^Is.*',   # e.g., IsActive
       r'^Has.*',  # e.g., HasInsurance
       r'^Can.*',  # e.g., CanSchedule
   ]
   
   DATETIME_PATTERNS = [
       r'Date.*',  # e.g., DateCreated
       r'.*Date$', # e.g., BirthDate
       r'.*Time.*',# e.g., TimeStamp
   ]
   ```

2. Implement column type inference:
   - Check against patterns
   - Consider MySQL data type
   - Analyze table relationships (for FKs)
   - Return transformation macro to use

3. Generate transformation code automatically:
   ```python
   # For foreign key
   {'source': '"ClaimNumPreAuth"', 'target': 'claim_preauth_id'}
   
   # For boolean
   convert_opendental_boolean('"IsActive"')
   
   # For datetime
   clean_opendental_date('"DateCreated"')
   ```

**Deliverable**: Intelligent column transformation inference

---

### Phase 3: Implement Update Generation (Week 3)

**Goal**: Auto-generate DBT update files for review

**Tasks**:
1. Create update file generators:
   - Source definition YAML generator
   - Staging model SQL generator
   - Schema documentation YAML generator

2. Generate organized update packages:
   ```
   docs/schema_updates/{table_name}_updates/
   ├── source_updates.yml       # Additions to _sources/*.yml
   ├── staging_updates.sql      # Additions to stg_*.sql
   ├── schema_docs.yml          # Additions to _stg_*.yml
   └── CHANGES.md               # Human-readable summary
   ```

3. Include in each update:
   - Exact line numbers for insertion
   - Context (surrounding code)
   - Diff view (before/after)
   - Recommended tests

**Deliverable**: Auto-generated update packages

---

### Phase 4: Implement Interactive Application Tool (Week 4)

**Goal**: Allow developer to review and apply updates safely

**Tasks**:
1. Create `apply_dbt_schema_updates.py` script:
   ```bash
   # Review all pending updates
   python scripts/apply_dbt_schema_updates.py --review
   
   # Apply updates for specific table
   python scripts/apply_dbt_schema_updates.py --table eobattach --apply
   
   # Apply all updates (with confirmation)
   python scripts/apply_dbt_schema_updates.py --apply-all
   ```

2. Features:
   - Interactive review (show diffs, allow edits)
   - Dry-run mode (show what would change)
   - Atomic application (all or nothing)
   - Automatic backup before changes
   - Validation with `dbt parse` after changes

3. Safety checks:
   - Backup all modified files
   - Validate YAML syntax
   - Validate SQL syntax (basic)
   - Run `dbt parse` to check for errors
   - Rollback on failure

**Deliverable**: Safe, interactive update application tool

---

### Phase 5: Integration with ETL Workflow (Week 5)

**Goal**: Integrate DBT schema checks into ETL workflow

**Tasks**:
1. Add DBT check to ETL pipeline:
   ```bash
   # After successful ETL run
   etl-run → tables.yml updated → CHECK DBT DRIFT
   ```

2. Add to `etl-init` command:
   ```python
   # In etl_pipeline/cli/commands.py
   @click.option('--check-dbt', is_flag=True, 
                 help='Check for DBT schema drift after initialization')
   def init(..., check_dbt):
       # ... existing init logic ...
       
       if check_dbt:
           click.echo("Checking DBT schema drift...")
           run_dbt_drift_check()
   ```

3. Create CI/CD integration:
   - Add schema drift check to GitHub Actions
   - Fail builds if drift detected without updates
   - Auto-create PRs with schema updates

**Deliverable**: Integrated schema governance workflow

---

## Best Practices

### ETL Schema Management

#### DO:
- ✅ Run schema analyzer before each major ETL update
- ✅ Review and commit `tables.yml` changes to version control
- ✅ Keep schema backups (analyzer does this automatically)
- ✅ Document OpenDental version with each schema update
- ✅ Use SCD type detection to maintain historical data correctly

#### DON'T:
- ❌ Manually edit column lists in `tables.yml` (use analyzer)
- ❌ Skip schema analysis after OpenDental updates
- ❌ Delete old schema backups (keep for audit trail)
- ❌ Ignore schema analysis warnings

---

### DBT Schema Management

#### DO:
- ✅ Always update all 4 layers for schema changes:
  1. Source definition (`_sources/*.yml`)
  2. Staging model (`stg_*.sql`)
  3. Model documentation (`_stg_*.yml`)
  4. Downstream models (if needed)

- ✅ Add appropriate tests for new columns:
  - Foreign keys → `relationships` test
  - Flags/booleans → `accepted_values` test
  - Dates → range validation tests

- ✅ Document column purpose and business context
- ✅ Run `dbt parse` to validate changes
- ✅ Run `dbt test` before merging
- ✅ Use consistent naming patterns:
  - Foreign keys: `*_id` suffix
  - Booleans: `is_*`, `has_*` prefix
  - Dates: `*_date`, `*_at` suffix

#### DON'T:
- ❌ Add columns to staging model without updating docs
- ❌ Skip relationship tests for foreign keys
- ❌ Use inconsistent naming (mix snake_case/PascalCase)
- ❌ Leave TODOs or incomplete transformations in code
- ❌ Commit without running `dbt parse` validation

---

### Column Type Pattern Matching

#### Foreign Keys
**Pattern**: Column name ends with `Num` or `ID` (excluding table's primary key)

**Examples**:
- `ClaimNumPreAuth` → `claim_preauth_id` (FK to claim table)
- `PatNum` → `patient_id` (FK to patient table)
- `ProvNum` → `provider_id` (FK to provider table)

**Transformation**:
```sql
{{ transform_id_columns([
    {'source': '"ClaimNumPreAuth"', 'target': 'claim_preauth_id'}
]) }}
```

**Tests**:
```yaml
tests:
  - relationships:
      to: ref('stg_opendental__claim')
      field: claim_id
      config:
        severity: warn
```

---

#### Booleans
**Pattern**: Column name starts with `Is`, `Has`, `Can`, `Should`

**Examples**:
- `IsActive` → `is_active`
- `HasInsurance` → `has_insurance`
- `CanSchedule` → `can_schedule`

**Transformation**:
```sql
{{ convert_opendental_boolean('"IsActive"') }} as is_active
```

**Tests**:
```yaml
tests:
  - accepted_values:
      values: [0, 1, true, false]
```

---

#### Dates/Timestamps
**Pattern**: Column name contains `Date`, `Time`, `Stamp`

**Examples**:
- `DateTCreated` → `_created_at`
- `DateTStamp` → `_updated_at`
- `BirthDate` → `birth_date`

**Transformation**:
```sql
{{ clean_opendental_date('"DateTCreated"') }} as _created_at
```

**Tests**:
```yaml
tests:
  - dbt_utils.expression_is_true:
      expression: ">= '2000-01-01'"
  - dbt_utils.expression_is_true:
      expression: "<= current_date"
```

---

### Handling Schema Breaking Changes

#### Column Removal
1. **Don't delete immediately** - deprecate first:
   ```sql
   -- Old column (deprecated, will be removed in v2.0)
   -- null as old_column_name,
   ```

2. **Check downstream dependencies**:
   ```bash
   # Search for column usage
   grep -r "old_column_name" dbt_dental_models/models/
   ```

3. **Create migration path**:
   - Document in model YAML
   - Add deprecation warning
   - Update downstream models first
   - Remove from staging model last

#### Column Rename
1. **Create alias during transition**:
   ```sql
   "NewColumnName" as new_column_name,
   "NewColumnName" as old_column_name,  -- Alias for backward compatibility
   ```

2. **Update downstream models gradually**
3. **Remove alias after all models updated**

#### Data Type Change
1. **Add new column with transformation**:
   ```sql
   "OldColumn"::new_type as old_column_new,
   "OldColumn" as old_column_legacy,  -- Keep for validation
   ```

2. **Validate both columns match** (in tests)
3. **Switch to new column** in downstream models
4. **Remove legacy column** after validation

---

## Troubleshooting Guide

### Issue: ETL Fails with "Unknown column" Error

**Symptoms**:
```
(1054, "Unknown column 'ClaimNumPreAuth' in 'field list'")
```

**Cause**: `tables.yml` references a column that doesn't exist in MySQL source

**Solution**:
1. Run schema analyzer to update configuration:
   ```bash
   cd etl_pipeline
   pipenv run python scripts/analyze_opendental_schema.py
   ```

2. Check if column was removed from OpenDental:
   ```sql
   -- In DBeaver (MySQL)
   DESCRIBE opendental.eobattach;
   ```

3. If column is truly gone, analyzer will remove it from `tables.yml`

---

### Issue: DBT Source Test Fails - Column Missing

**Symptoms**:
```
Database Error in source opendental.eobattach
  column "ClaimNumPreAuth" does not exist
```

**Cause**: DBT source definition references column not in PostgreSQL raw table

**Solution**:
1. Check if column exists in PostgreSQL:
   ```sql
   -- In DBeaver (PostgreSQL)
   SELECT column_name 
   FROM information_schema.columns 
   WHERE table_schema = 'raw' 
   AND table_name = 'eobattach';
   ```

2. If missing, re-run ETL to create column:
   ```bash
   cd etl_pipeline
   etl-init
   etl-run --tables eobattach
   ```

3. Update DBT source definition to match reality

---

### Issue: DBT Compilation Fails - Undefined Column

**Symptoms**:
```
Compilation Error in model stg_opendental__eobattach
  column "claim_preauth_id" does not exist
```

**Cause**: Staging model references a column not selected in the CTE

**Solution**:
1. Check staging model SQL for missing column transformation
2. Add transformation to `renamed_columns` CTE:
   ```sql
   {{ transform_id_columns([
       {'source': '"ClaimNumPreAuth"', 'target': 'claim_preauth_id'}
   ]) }}
   ```

3. Verify source has the column:
   ```sql
   -- In source_data CTE
   select * from {{ source('opendental', 'eobattach') }}
   ```

---

### Issue: Schema Drift Not Detected

**Symptoms**: Schema analyzer doesn't detect new columns

**Cause**: Cache or configuration issue

**Solution**:
1. Clear analyzer cache (if implemented)
2. Check MySQL connection:
   ```bash
   cd etl_pipeline
   etl-init
   # Verify connection works
   ```

3. Manually check for new columns:
   ```sql
   -- Compare column counts
   SELECT COUNT(*) FROM information_schema.columns 
   WHERE table_schema = 'opendental' AND table_name = 'eobattach';
   ```

4. Run analyzer with verbose logging:
   ```bash
   python scripts/analyze_opendental_schema.py --verbose
   ```

---

### Issue: Downstream Model Fails After Schema Update

**Symptoms**: Intermediate/mart models fail after adding new column to staging

**Cause**: Downstream model expects different schema or has hardcoded column list

**Solution**:
1. Find affected models:
   ```bash
   grep -r "stg_opendental__eobattach" dbt_dental_models/models/
   ```

2. Check if they use `select *` (should auto-include new column):
   ```sql
   select * from {{ ref('stg_opendental__eobattach') }}  -- Good, will include new column
   ```

3. If they have explicit column lists, update them:
   ```sql
   select
       eob_attach_id,
       claim_payment_id,
       claim_preauth_id,  -- ADD NEW COLUMN
       file_name
   from {{ ref('stg_opendental__eobattach') }}
   ```

---

## Appendix A: File Update Checklist

When adding a new column to a DBT model, use this checklist:

### ✅ Step 1: Update Source Definition
**File**: `models/staging/opendental/_sources/{domain}_sources.yml`

- [ ] Add column under appropriate table
- [ ] Include description
- [ ] Add tests if appropriate (`not_null_quoted`, `unique_quoted`)
- [ ] Maintain alphabetical or logical order

### ✅ Step 2: Update Staging Model
**File**: `models/staging/opendental/stg_opendental__{table}.sql`

- [ ] Add column transformation in appropriate section:
  - Primary/Foreign Keys → `transform_id_columns()`
  - Booleans → `convert_opendental_boolean()`
  - Dates → `clean_opendental_date()`
  - Other → Direct mapping with `as` alias
- [ ] Use proper macro for data type
- [ ] Follow snake_case naming convention
- [ ] Add comment if transformation is complex

### ✅ Step 3: Update Model Documentation
**File**: `models/staging/opendental/_stg_opendental__{table}.yml`

- [ ] Add column documentation under `columns:`
- [ ] Include description with business context
- [ ] Add appropriate tests:
  - `not_null` for required columns
  - `unique` for unique columns
  - `relationships` for foreign keys
  - `accepted_values` for enums/flags
  - `positive_values` for numeric IDs
- [ ] Document known issues or caveats
- [ ] Maintain order matching staging model

### ✅ Step 4: Validate Changes
- [ ] Run `dbt parse` - check for syntax errors
- [ ] Run `dbt compile -m stg_opendental__{table}` - check compiled SQL
- [ ] Run `dbt run -m stg_opendental__{table}` - check execution
- [ ] Run `dbt test -m stg_opendental__{table}` - check tests pass

### ✅ Step 5: Check Downstream Impact
- [ ] Search for model references:
  ```bash
  grep -r "stg_opendental__{table}" dbt_dental_models/models/
  ```
- [ ] Review intermediate models using this staging model
- [ ] Review mart models using this staging model
- [ ] Update downstream models if they need new column
- [ ] Re-run downstream model tests

### ✅ Step 6: Commit and Document
- [ ] Commit changes with descriptive message:
  ```
  feat(dbt): Add claim_preauth_id to eobattach staging model
  
  - Added ClaimNumPreAuth column from source
  - Transformed to claim_preauth_id foreign key
  - Added relationship test to stg_opendental__claim
  - Updated documentation with business context
  ```
- [ ] Update CHANGELOG if maintaining one
- [ ] Document in schema evolution log

---

## Appendix B: Command Reference

### Schema Analysis Commands

```bash
# Analyze MySQL schema and update tables.yml
python scripts/analyze_opendental_schema.py

# Analyze specific tables only
python scripts/analyze_opendental_schema.py --tables eobattach,claim,patient

# Check for DBT schema drift (future)
python scripts/analyze_opendental_schema.py --check-dbt

# Generate DBT update packages (future)
python scripts/analyze_opendental_schema.py --check-dbt --generate-updates
```

### DBT Update Commands (Future)

```bash
# Review pending schema updates
python scripts/apply_dbt_schema_updates.py --review

# Apply updates for specific table
python scripts/apply_dbt_schema_updates.py --table eobattach --apply

# Dry-run (show what would change)
python scripts/apply_dbt_schema_updates.py --table eobattach --dry-run

# Apply all pending updates
python scripts/apply_dbt_schema_updates.py --apply-all
```

### DBT Validation Commands

```bash
# Parse all models (syntax check)
dbt parse

# Compile specific model (check SQL generation)
dbt compile -m stg_opendental__eobattach

# Run specific model (check execution)
dbt run -m stg_opendental__eobattach

# Test specific model (check data quality)
dbt test -m stg_opendental__eobattach

# Run model and all downstream dependencies
dbt run -m stg_opendental__eobattach+
```

---

## Appendix C: Future Enhancements

### Short-term (1-3 months)
1. **Automated drift detection** - Daily schema comparison job
2. **Update package generation** - Auto-generate DBT update files
3. **Interactive application tool** - Safe way to apply updates
4. **CI/CD integration** - Fail builds on unhandled schema drift

### Medium-term (3-6 months)
1. **Foreign key relationship detection** - Auto-detect FK targets
2. **Enum/accepted values inference** - Auto-detect value sets from data
3. **Column description generation** - Use AI to generate descriptions
4. **Impact analysis** - Show which downstream models affected

### Long-term (6-12 months)
1. **Full AI-assisted automation** - LLM-powered schema updates
2. **Automated migration scripts** - Generate ALTER TABLE scripts
3. **Zero-downtime schema changes** - Blue/green model deployments
4. **Schema version management** - Track schema versions over time

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-10-07 | Data Engineering | Initial strategy document |

---

## Related Documentation

- [ETL Schema Update Command Feature](etl_schema_update_command_feature.md)
- [Schema Analysis SCD Improvements](schema_analysis_scd_improvements.md)
- [DBT Project Documentation](../../dbt_dental_models/README.md)
- [ETL Pipeline Documentation](../../etl_pipeline/README.md)

