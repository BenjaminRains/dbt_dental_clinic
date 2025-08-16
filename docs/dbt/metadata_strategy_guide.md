# Metadata Strategy Guide for Dental Clinic Data Pipeline

## Overview

This document defines the metadata strategy for all model layers in the dbt_dental_clinic project.
 Our approach prioritizes **business timestamps** over technical processing timestamps, while
  maintaining essential pipeline tracking for debugging and monitoring.

## Core Principles

### 1. Business-First Metadata
- **Primary focus**: Business timestamps from OpenDental source system
- **Secondary focus**: ETL extraction timestamps for data lineage
- **Essential focus**: dbt processing timestamps for pipeline debugging and monitoring

### 2. Single Source of Truth
- Each model layer preserves metadata from its primary source
- Clear documentation of metadata availability in YAML files
- Primary source metadata takes precedence over secondary sources

### 3. Graceful Degradation
- Missing metadata fields are set to NULL
- YAML documentation notes metadata limitations
- No artificial default values for missing metadata

## Data Lifecycle Context

### Dental Clinic Pipeline Timing
```
Nightly/Semi-nightly ETL Run → dbt Build (immediately after)
     ↓                              ↓
OpenDental → ETL Pipeline → Staging Models → Intermediate Models → Marts
     ↓              ↓              ↓              ↓              ↓
  Business      ETL timestamps   Staging       Intermediate    Final data
  timestamps                     metadata      metadata
```

**Key Insight**: While ETL and dbt builds are synchronized within ~15 minutes, both `_loaded_at`
 and `_transformed_at` serve distinct purposes for pipeline monitoring and debugging.

## Metadata Fields Definition

### Standard Metadata Fields
```sql
_loaded_at      -- ETL extraction timestamp (when data was loaded to raw schema)
_transformed_at -- dbt model build timestamp (when this specific model was built)
_created_at     -- Business creation timestamp (when record was created in OpenDental)
_updated_at     -- Business update timestamp (when record was last updated in OpenDental)
_created_by     -- Business user ID (who created the record in OpenDental)
```

### Field Priorities
1. **High Priority**: `_created_at`, `_updated_at` (business timestamps)
2. **Medium Priority**: `_created_by` (business user tracking)
3. **Essential for Debugging**: `_loaded_at`, `_transformed_at` (pipeline monitoring)

### Pipeline Debugging Value
```sql
-- Normal operation:
_loaded_at = '2024-01-15 02:00:00'      -- ETL succeeded
_transformed_at = '2024-01-15 02:15:00'  -- dbt succeeded

-- ETL failure:
_loaded_at = '2024-01-14 02:00:00'      -- Old ETL timestamp
_transformed_at = '2024-01-15 02:15:00'  -- Recent dbt timestamp

-- dbt failure:
_loaded_at = '2024-01-15 02:00:00'      -- Recent ETL timestamp
_transformed_at = '2024-01-14 02:15:00'  -- Old dbt timestamp
```

## Staging Model Metadata

### Implementation Pattern
```sql
-- In staging models
{{ standardize_metadata_columns(
    created_at_column='"DateEntry"',      -- OpenDental creation date
    updated_at_column='"DateTStamp"',     -- OpenDental update date
    created_by_column='"UserNumEntry"'    -- OpenDental user ID
) }}
```

### Example: Patient Staging Model
```sql
-- stg_opendental__patient.sql
{{ standardize_metadata_columns(
    created_at_column='"DateEntry"',
    updated_at_column='"DateTStamp"',
    created_by_column='"UserNumEntry"'
) }}
```

### User ID Column Handling Pattern
For models that need to preserve user ID columns for compatibility with existing table structures, use this pattern:

```sql
-- User ID column (using transform_id_columns for proper type conversion)
{{ transform_id_columns([
    {'source': '"SecUserNumEntry"', 'target': 'sec_user_num_entry'}
]) }},

-- Standardized metadata columns (without created_by to avoid duplication)
{{ standardize_metadata_columns(
    created_at_column='"SecDateEntry"',
    updated_at_column='"SecDateTEdit"'
) }},

-- User ID column for compatibility with existing table structure
{{ transform_id_columns([
    {'source': '"SecUserNumEntry"', 'target': '_created_by'}
]) }}
```

**Key Points:**
1. **Separate user ID columns**: Create both `sec_user_num_entry` and `_created_by` for different purposes
2. **Use transform_id_columns**: Ensures proper type conversion and case sensitivity handling
3. **Avoid duplication**: Don't pass `created_by_column` to `standardize_metadata_columns` when creating separate user ID columns
4. **Maintain compatibility**: Keep `_created_by` for existing table structures while adding business-specific user ID columns

**Primary Purpose - Data Lineage and Traceability:**
The main purpose of this pattern is to **clearly indicate where each column came from**. This provides:
- **Full audit trail**: Every column can be traced back to its source
- **Data provenance**: Clear documentation of data origins for compliance and debugging
- **Flexibility for downstream use**: Columns can be removed or cleaned later for BI datasets or frontend users
- **Healthcare compliance**: Critical for HIPAA and other regulatory requirements where data lineage is mandatory

### YAML Documentation
```yaml
# _stg_opendental__patient.yml
- name: _loaded_at
  description: >
    ETL extraction timestamp - when this record was loaded to raw schema
    Source: ETL pipeline metadata
    Purpose: Data lineage tracking and pipeline monitoring
  tests:
    - not_null

- name: _transformed_at
  description: >
    dbt model build timestamp - when this staging model was built
    Source: dbt model execution
    Purpose: Model-specific tracking and pipeline debugging
  tests:
    - not_null

- name: _created_at
  description: >
    Business creation timestamp - when this patient was created in OpenDental
    Source: OpenDental "DateEntry" field
    Purpose: Business timeline analysis and audit trail
  tests:
    - not_null

- name: _updated_at
  description: >
    Business update timestamp - when this patient was last updated in OpenDental
    Source: OpenDental "DateTStamp" field
    Purpose: Change tracking and incremental loading support
  tests:
    - not_null

- name: _created_by
  description: >
    Business user ID - who created this patient in OpenDental
    Source: OpenDental "UserNumEntry" field
    Purpose: User accountability and audit trail
  tests:
    - not_null
```

## Intermediate Model Metadata

### Implementation Pattern
```sql
-- In intermediate models
-- Preserve metadata from primary source, set others to NULL if unavailable

-- Primary source metadata (preserved)
primary_source._loaded_at,
primary_source._transformed_at,
primary_source._created_at,
primary_source._updated_at,
primary_source._created_by,

-- Secondary source metadata (NULL if unavailable)
secondary_source._loaded_at as secondary_loaded_at,  -- NULL if not available
secondary_source._transformed_at as secondary_transformed_at,  -- NULL if not available
secondary_source._created_at as secondary_created_at,  -- NULL if not available
secondary_source._updated_at as secondary_updated_at,  -- NULL if not available
secondary_source._created_by as secondary_created_by   -- NULL if not available
```

### Example: Insurance Coverage Intermediate Model
```sql
-- int_insurance_coverage.sql
Final as (
    select
        -- Business logic fields...
        
        -- Primary source metadata (insurance plan)
        ip._loaded_at,
        ip._transformed_at,
        ip._created_at,
        ip._updated_at,
        ip._created_by,
        
        -- Secondary source metadata (subscriber - may be NULL)
        s._loaded_at as subscriber_loaded_at,
        s._transformed_at as subscriber_transformed_at,
        s._created_at as subscriber_created_at,
        s._updated_at as subscriber_updated_at,
        s._created_by as subscriber_created_by,
        
        -- Intermediate model build timestamp
        current_timestamp as _transformed_at
        
    from PatientPlan pp
    left join InsurancePlan ip on pp.patplan_id = ip.insurance_plan_id
    left join Subscriber s on pp.insurance_subscriber_id = s.subscriber_id
)
```

### Using the Macro
```sql
-- With the updated macro
{{ standardize_intermediate_metadata(primary_source_alias='ip') }}

-- This generates:
-- ip._loaded_at,
-- ip._created_at,
-- ip._updated_at,
-- ip._created_by,
-- current_timestamp as _transformed_at
```

### YAML Documentation
```yaml
# _int_insurance_coverage.yml
- name: _loaded_at
  description: >
    ETL extraction timestamp from primary source (insurance plan)
    Source: stg_opendental__insplan._loaded_at
    Purpose: Data lineage tracking
  tests:
    - not_null

- name: _transformed_at
  description: >
    dbt intermediate model build timestamp - when this model was built
    Source: dbt model execution
    Purpose: Model-specific tracking and pipeline debugging
  tests:
    - not_null

- name: _created_at
  description: >
    Business creation timestamp from primary source (insurance plan)
    Source: stg_opendental__insplan._created_at
    Purpose: Business timeline analysis
  tests:
    - not_null

- name: _updated_at
  description: >
    Business update timestamp from primary source (insurance plan)
    Source: stg_opendental__insplan._updated_at
    Purpose: Change tracking
  tests:
    - not_null

- name: _created_by
  description: >
    Business user ID from primary source (insurance plan)
    Source: stg_opendental__insplan._created_by
    Purpose: User accountability
  tests:
    - not_null

- name: subscriber_loaded_at
  description: >
    ETL extraction timestamp from secondary source (subscriber)
    Source: stg_opendental__inssub._loaded_at
    Note: May be NULL if subscriber data is unavailable
    Purpose: Secondary data lineage tracking
  tests:
    - dbt_utils.expression_is_true:
        expression: "subscriber_loaded_at IS NOT NULL OR subscriber_id = -1"
        config:
          severity: warn
          description: "Subscriber metadata should be available when subscriber exists"

- name: subscriber_transformed_at
  description: >
    dbt model build timestamp from secondary source (subscriber)
    Source: stg_opendental__inssub._transformed_at
    Note: May be NULL if subscriber data is unavailable
    Purpose: Secondary model tracking
  tests:
    - dbt_utils.expression_is_true:
        expression: "subscriber_transformed_at IS NOT NULL OR subscriber_id = -1"
        config:
          severity: warn
          description: "Subscriber metadata should be available when subscriber exists"
```

## Marts Model Metadata

### Implementation Pattern
```sql
-- In marts models
-- Preserve metadata from primary intermediate source
-- Add business-relevant aggregations

-- Primary source metadata
primary_intermediate._loaded_at,
primary_intermediate._transformed_at,
primary_intermediate._created_at,
primary_intermediate._updated_at,
primary_intermediate._created_by,

-- Business aggregations
min(related_records._created_at) as earliest_related_created_at,
max(related_records._updated_at) as latest_related_updated_at
```

### Example: Patient Mart Model
```sql
-- marts_patient_profile.sql
patient_profile as (
    select
        -- Business logic fields...
        
        -- Primary source metadata
        ip._loaded_at,
        ip._transformed_at,
        ip._created_at,
        ip._updated_at,
        ip._created_by,
        
        -- Business aggregations
        min(pp._created_at) as earliest_plan_created_at,
        max(pp._updated_at) as latest_plan_updated_at,
        
        -- Mart model build timestamp
        current_timestamp as _transformed_at
        
    from int_patient_profile ip
    left join int_insurance_coverage ic on ip.patient_id = ic.patient_id
    left join int_payment_split ps on ip.patient_id = ps.patient_id
)
```

## Handling Missing Metadata

### Graceful Degradation Strategy
```sql
-- When metadata is unavailable, set to NULL
-- No artificial defaults or fallback values

-- Example: Some staging models may not have _created_by
{{ standardize_metadata_columns(
    created_at_column='"DateEntry"',
    updated_at_column='"DateTStamp"'
    -- No created_by_column specified - will be NULL
) }}
```

### YAML Documentation for Missing Metadata
```yaml
- name: _created_by
  description: >
    Business user ID - who created this record in OpenDental
    Source: OpenDental "UserNumEntry" field
    Note: This field is NULL for this model as the source table does not track user creation
    Purpose: User accountability (when available)
  tests:
    - dbt_utils.expression_is_true:
        expression: "_created_by IS NULL"
        config:
          severity: warn
          description: "This model does not track user creation - field is intentionally NULL"
```

## Pipeline Debugging and Monitoring

### Key Use Cases for `_loaded_at` and `_transformed_at`

1. **Pipeline Health Monitoring:**
   ```sql
   -- Check for pipeline delays
   SELECT 
       model_name,
       _loaded_at,
       _transformed_at,
       EXTRACT(EPOCH FROM (_transformed_at - _loaded_at))/60 as minutes_delay
   FROM staging_models
   WHERE _transformed_at - _loaded_at > INTERVAL '30 minutes'
   ```

2. **Model Dependency Issues:**
   ```sql
   -- Identify models using stale data
   SELECT 
       intermediate_model,
       staging_model,
       intermediate._transformed_at as intermediate_build_time,
       staging._transformed_at as staging_build_time
   FROM intermediate_models
   JOIN staging_models ON intermediate.source = staging.name
   WHERE intermediate._transformed_at > staging._transformed_at + INTERVAL '1 hour'
   ```

3. **Incremental Processing Validation:**
   ```sql
   -- Verify incremental models are processing recent data
   SELECT 
       model_name,
       MAX(_transformed_at) as last_build_time,
       CURRENT_TIMESTAMP - MAX(_transformed_at) as time_since_last_build
   FROM all_models
   GROUP BY model_name
   HAVING CURRENT_TIMESTAMP - MAX(_transformed_at) > INTERVAL '24 hours'
   ```

## Best Practices

### 1. Primary Source Selection
- Choose the most business-relevant source as primary
- Document the selection rationale in YAML comments
- Use consistent primary source across related models

### 2. PostgreSQL Case Sensitivity Handling
When working with PostgreSQL and case-sensitive column names from OpenDental:

**Problem**: OpenDental uses CamelCase column names (e.g., `"SecUserNumEntry"`) that are preserved in the raw schema, but dbt-generated SQL may reference them without proper quoting.

**Solution**: Always use `transform_id_columns` macro for user ID and foreign key columns:

```sql
-- ❌ Incorrect - may cause case sensitivity issues
"SecUserNumEntry" as sec_user_num_entry,

-- ✅ Correct - uses transform_id_columns for proper handling
{{ transform_id_columns([
    {'source': '"SecUserNumEntry"', 'target': 'sec_user_num_entry'}
]) }}
```

**Key Benefits**:
- Ensures proper quoting and case preservation
- Handles type conversion consistently
- Provides standardized error handling for invalid IDs
- Maintains compatibility with existing table structures

### 3. Metadata Documentation
- Always document metadata field availability in YAML
- Note when fields are intentionally NULL
- Explain the business purpose of each metadata field

### 4. Testing Strategy
- Test that primary metadata is not NULL
- Warn when secondary metadata is unexpectedly NULL
- Document expected NULL patterns

### 5. Naming Conventions
- Primary metadata: `_loaded_at`, `_transformed_at`, `_created_at`, `_updated_at`, `_created_by`
- Secondary metadata: `{source}_loaded_at`, `{source}_transformed_at`, `{source}_created_at`, etc.
- Business aggregations: `earliest_{field}`, `latest_{field}`, etc.

### 6. Pipeline Monitoring
- Monitor `_loaded_at` vs `_transformed_at` differences
- Set up alerts for pipeline delays
- Track model build dependencies using timestamps

## Migration Guidelines

### For Existing Models
1. **Identify primary source** for each model
2. **Preserve primary metadata** from staging/intermediate sources
3. **Set secondary metadata to NULL** if unavailable
4. **Update YAML documentation** to reflect metadata availability
5. **Include both `_loaded_at` and `_transformed_at`** for pipeline monitoring

### Common Migration Patterns
```sql
-- Before (inconsistent metadata)
current_timestamp as _loaded_at,           -- Redundant
current_timestamp as _transformed_at,      -- Redundant
manual_created_at,                         -- Inconsistent

-- After (business-focused with pipeline tracking)
primary_source._loaded_at,                 -- ETL extraction time
primary_source._transformed_at,            -- Source model build time
primary_source._created_at,                -- Business creation time
primary_source._updated_at,                -- Business update time
primary_source._created_by,                -- Business user
current_timestamp as _transformed_at       -- This model build time
```

## Summary

This metadata strategy prioritizes business timestamps while maintaining essential pipeline tracking
 for debugging and monitoring. Both `_loaded_at` and `_transformed_at` serve distinct purposes and
  are valuable for understanding data pipeline health and troubleshooting issues.

Key principles:
- **Business timestamps first** (`_created_at`, `_updated_at`)
- **Pipeline tracking essential** (`_loaded_at`, `_transformed_at`)
- **Single source of truth** for primary metadata
- **Graceful degradation** for missing metadata
- **Clear documentation** of metadata availability
- **Comprehensive pipeline monitoring** capabilities

## Downstream Flexibility

### Data Lineage Preservation
The architecture preserves complete data lineage while allowing flexibility for different downstream consumers:

```sql
-- Staging layer: Full lineage preserved
sec_user_num_entry,      -- Business-specific user ID
_created_by,             -- Standardized metadata column
_loaded_at,              -- ETL extraction timestamp
_transformed_at,         -- dbt build timestamp

-- Intermediate layer: Can choose which columns to preserve
primary_source._created_by,           -- Keep for audit trail
secondary_source._created_by as secondary_created_by,  -- Optional

-- Marts layer: Clean for BI consumption
-- _created_by removed for cleaner BI datasets
-- Only business-relevant columns included
```

### Consumer-Specific Views
Different consumers can access appropriately cleaned data:

- **Analysts/BI**: Clean marts with business-relevant columns only
- **Compliance/Audit**: Full staging models with complete lineage
- **Frontend Applications**: Intermediate models with selected metadata
- **Data Science**: Raw staging with all available metadata

This approach ensures that **data lineage is never lost** while providing the flexibility to present clean, consumer-appropriate datasets.
