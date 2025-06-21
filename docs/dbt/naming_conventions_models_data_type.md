# Consolidated Naming Conventions - dbt_dental_clinic

## Overview

This document defines the standardized naming conventions and data type transformations for the dbt_dental_clinic project. It works in conjunction with:
- **`macro_implementation_checklist.md`** - Provides a detailed checklist for implementing these conventions in staging models
- **`naming_conventions_implementation_strategy.md`** - Outlines the phased migration plan for implementing these standards across the entire project

Together, these documents provide a comprehensive framework for standardizing data models and transformations.

---

## Core Principles

1. **Consistency First**: Use the same convention for similar objects across the entire pipeline
2. **Clear Context**: Names should indicate data lineage and processing stage
3. **Tool Alignment**: Follow established conventions for dbt, PostgreSQL, and Python
4. **Minimal Cognitive Load**: Reduce the number of different naming patterns

## Database and Schema Naming

### Environment Variables (Standardized)
```bash
# Source Database (OpenDental Production)
OPENDENTAL_SOURCE_HOST=client-server
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=opendental_source
OPENDENTAL_SOURCE_USER=readonly_user

# Replication Database (Local MySQL)
MYSQL_REPLICATION_HOST=localhost
MYSQL_REPLICATION_PORT=3305
MYSQL_REPLICATION_DB=opendental_replication
MYSQL_REPLICATION_USER=replication_user

# Analytics Database (PostgreSQL)
POSTGRES_ANALYTICS_HOST=localhost
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_DB=opendental_analytics
POSTGRES_ANALYTICS_USER=analytics_user
```

### Connection Factory Functions
```python
# Clear, technology-specific naming
get_opendental_source_connection()    # OpenDental MySQL (read-only)
get_mysql_replication_connection()    # Local MySQL (full access)
get_postgres_analytics_connection()   # PostgreSQL analytics DB
```

## SQL Naming Conventions

### 1. Raw Database Columns
**Rule**: Use exact CamelCase as they appear in OpenDental
```sql
"PatNum", "ClaimNum", "DateService", "IsHidden"
```

### 2. Transformed/Derived Fields  
**Rule**: Use snake_case for all transformed fields
```sql
patient_id, claim_id, service_date, is_hidden, total_payment
```

### 3. CTEs (Revised Convention)
**Rule**: Use snake_case for ALL CTEs (aligns with dbt best practices)
```sql
with source_data as (
    select * from {{ ref('stg_opendental__patient') }}
),

renamed_columns as (
    select
        "PatNum" as patient_id,
        "DateEntry" as created_date
    from source_data
),

payment_summary as (
    select
        patient_id,
        sum(payment_amount) as total_payments
    from renamed_columns
    group by patient_id
)
```

### 4. File and Model Names
**Rule**: Consistent snake_case across all files
```
# SQL files
stg_opendental__patient.sql
int_patient_demographics.sql
mart_financial_performance.sql

# Python files
mysql_replication_extractor.py
postgres_analytics_loader.py
```

## Data Type Standardization

### Boolean Conversion Macro
```sql
{% macro convert_opendental_boolean(column_name) %}
    CASE 
        WHEN {{ column_name }} = 1 THEN true
        WHEN {{ column_name }} = 0 THEN false
        ELSE null 
    END
{% endmacro %}

-- Usage
{{ convert_opendental_boolean('"IsHidden"') }} as is_hidden
```

### ID Column Transformation
```sql
-- Standard pattern for all ID columns
"PatNum" as patient_id,     -- source_field + _id suffix
"ClaimNum" as claim_id,     -- Remove 'Num', add 'id'
"ProcNum" as procedure_id   -- Consistent across all models
```

**CRITICAL RULE**: All OpenDental columns ending in "Num" are ID fields and MUST be transformed to
 snake_case with "_id" suffix. Exceptions can be considered on a case by case basis.

Examples:
- `"CanadianNetworkNum"` → `canadian_network_id` (NOT `canadian_network_num`)
- `"FeatureNum"` → `feature_id` (NOT `feature_num`)
- `"CategoryNum"` → `category_id` (NOT `category_num`)

## Metadata Strategy (Simplified)

### Single Metadata Approach
Add all metadata columns at the earliest transformation point:

```sql
-- In staging models
select
    -- Business columns
    "PatNum" as patient_id,
    "DateEntry" as created_date,
    
    -- Metadata columns (standardized across all models)
    _extracted_at,                    -- From ETL pipeline
    "DateEntry" as _created_at,       -- Original creation timestamp
    coalesce("DateTStamp", "DateEntry") as _updated_at,  -- Last update
    current_timestamp as _transformed_at  -- dbt processing time
from {{ source('opendental', 'patient') }}
```

### Incremental Model Configuration
```sql
{{ config(
    materialized='incremental',
    unique_key='patient_id',
    on_schema_change='fail',
    incremental_strategy='merge'
) }}

{% if is_incremental() %}
  where _updated_at > (select max(_updated_at) from {{ this }})
{% endif %}
```

## Schema Organization

### PostgreSQL Schema Structure
```sql
-- Raw data from ETL pipeline
raw.patient
raw.appointment  
raw.treatment

-- dbt staging models
staging.stg_opendental__patient
staging.stg_opendental__appointment
staging.stg_opendental__treatment

-- dbt intermediate models
intermediate.int_patient_demographics
intermediate.int_appointment_metrics

-- dbt mart models
marts.dim_patient
marts.fact_appointments
marts.mart_financial_performance
```

## dbt Configuration Updates

### Staging Model Configuration
**Rule**: All staging models MUST include a config block with schema specification

```sql
-- For transactional/fact tables (with timestamps for tracking changes)
{{ config(
    materialized='incremental',
    unique_key='<primary_key>_id',
    schema='staging'
) }}

-- For reference/dimension tables (slowly changing or static data)
{{ config(
    materialized='view',
    schema='staging'
) }}
```

**Guidelines for choosing materialization:**
- Use `incremental` for:
  - Tables with DateTStamp or similar update tracking columns
  - High-volume transactional data (appointments, procedures, payments)
  - Tables that benefit from processing only new/changed records
  
- Use `view` for:
  - Reference/lookup tables (definitions, types, categories)
  - Tables without timestamp columns
  - Low-volume or rarely changing data
  - Tables that join to others for timestamp metadata

**Important**: The `schema='staging'` parameter is REQUIRED in all staging model configs to ensure proper schema organization.

### Updated dbt_project.yml
```yaml
models:
  dbt_dental_clinic:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: table  
      +schema: intermediate
    marts:
      +materialized: table
      +schema: marts
```

### Source Configuration
```yaml
# sources.yml
sources:
  - name: opendental
    description: "Raw OpenDental data from ETL pipeline"
    database: opendental_analytics
    schema: raw
    tables:
      - name: patient
        columns:
          - name: PatNum
            description: "Patient number (primary key in OpenDental)"
          - name: _extracted_at
            description: "When record was extracted from source"
```

## Python ETL Code Updates

### Class Structure
```python
class ELTPipeline:
    def __init__(self):
        # Clear, descriptive engine names
        self.opendental_source_engine = None
        self.mysql_replication_engine = None  
        self.postgres_analytics_engine = None
        
        # Standardized database names
        self.source_db = "opendental_source"
        self.replication_db = "opendental_replication"
        self.analytics_db = "opendental_analytics"
```

### Tracking Tables
```sql
-- MySQL replication tracking
opendental_replication.etl_extraction_log
- table_name
- extraction_started_at
- extraction_completed_at
- rows_extracted
- status

-- PostgreSQL analytics tracking  
raw.etl_transformation_log
- table_name
- transformation_started_at
- transformation_completed_at
- rows_transformed
- status
```

## Benefits of This Approach

1. **Reduced Complexity**: Single case convention per context (snake_case for CTEs, files, Python)
2. **Better Tool Alignment**: Follows dbt and PostgreSQL best practices
3. **Clearer Data Lineage**: Consistent metadata across all stages
4. **Easier Maintenance**: Fewer special cases and exceptions
5. **Improved Readability**: More consistent visual patterns
6. **Better IDE Support**: snake_case works better with most SQL formatters

## Migration Strategy

1. **Phase 1**: Update new models with revised conventions
2. **Phase 2**: Create macros for common patterns (boolean conversion, metadata)
3. **Phase 3**: Gradually refactor existing CTEs to snake_case
4. **Phase 4**: Update environment variables and connection functions
5. **Phase 5**: Consolidate metadata strategy across all models

This approach maintains your excellent distinction between raw and derived data while simplifying the overall convention set.