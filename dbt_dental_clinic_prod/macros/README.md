# dbt Macros for Improved Naming Conventions

This directory contains helper macros that implement the improved naming conventions as outlined in the Implementation Strategy (Phase 1.2). These macros standardize common transformation patterns and ensure consistency across all dbt models.

## Core Transformation Macros

### `convert_opendental_boolean(column_name)`
**Purpose:** Converts OpenDental's 0/1 integer boolean fields to proper PostgreSQL boolean values.

**Parameters:**
- `column_name` (str): The quoted column name from OpenDental (e.g., `'"IsHidden"'`)

**Usage:**
```sql
select
    {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
    {{ convert_opendental_boolean('"TxtMsgOk"') }} as text_messaging_consent
from source_table
```

**Output:** SQL expression that returns `true`/`false`/`null`

---

### `standardize_metadata_columns()`
**Purpose:** Adds consistent metadata columns across all staging models for data lineage tracking.

**Parameters:**
- `created_at_column` (str, optional): Source column for creation timestamp (default: `'"DateEntry"'`)
- `updated_at_column` (str, optional): Source column for update timestamp (default: `'"DateTStamp"'`)
- `created_by_column` (str, optional): Source column for creating user ID (default: `'"SecUserNumEntry"'`)

**Usage:**
```sql
select
    -- Your business columns here
    "PatNum" as patient_id,
    
    -- Add standardized metadata
    {{ standardize_metadata_columns() }}
from source_table
```

**Custom usage:**
```sql
{{ standardize_metadata_columns(
    created_at_column='"SecDateEntry"',
    updated_at_column='"SecDateTEdit"'
) }}
```

**Output:** Adds four columns: `_loaded_at`, `_created_at`, `_updated_at`, `_created_by_user_id`

---

### `transform_id_columns(transformations)`
**Purpose:** Standardizes ID column transformations from OpenDental naming to snake_case.

**Parameters:**
- `transformations` (list): List of dicts with `source` and `target` keys

**Usage:**
```sql
select
    {{ transform_id_columns([
        {'source': '"PatNum"', 'target': 'patient_id'},
        {'source': '"ClinicNum"', 'target': 'clinic_id'},
        {'source': '"PriProv"', 'target': 'primary_provider_id'}
    ]) }}
from source_table
```

---

## Data Cleaning Macros

### `clean_opendental_date(column_name, default_null=true)`
**Purpose:** Handles OpenDental's inconsistent date formats and null date representations.

**Parameters:**
- `column_name` (str): The quoted column name from OpenDental
- `default_null` (bool): Whether to return null for invalid dates (default: true)

**Usage:**
```sql
select
    {{ clean_opendental_date('"DateEntry"') }} as entry_date,
    {{ clean_opendental_date('"DateTimeDeceased"') }} as deceased_datetime
from source_table
```

**Invalid dates handled:** `'0001-01-01'`, `'1900-01-01'`, and their timestamp equivalents

---

### `clean_opendental_string(column_name, trim_whitespace=true)`
**Purpose:** Cleans OpenDental string columns by handling empty strings and whitespace.

**Parameters:**
- `column_name` (str): The quoted column name from OpenDental
- `trim_whitespace` (bool): Whether to trim whitespace (default: true)

**Usage:**
```sql
select
    {{ clean_opendental_string('"CheckNum"') }} as check_number,
    {{ clean_opendental_string('"PatNote"', false) }} as patient_notes
from source_table
```

---

## Template and Structure Macros

### `staging_model_cte_structure(source_name, table_name)`
**Purpose:** Provides standardized CTE structure for new staging models.

**Parameters:**
- `source_name` (str): Source name (e.g., 'opendental')
- `table_name` (str): Table name (e.g., 'patient')

**Usage:**
```sql
{{ staging_model_cte_structure('opendental', 'patient') }}
```

**Output:** Complete staging model template with snake_case CTEs and proper structure

---

## Validation and Testing Macros

### `validate_id_transformation(old_column, new_column, table_ref)`
**Purpose:** Validates ID column transformations during migration.

**Usage:**
```sql
{{ validate_id_transformation('"PatNum"', 'patient_id', 'stg_opendental__patient') }}
```

---

### `validate_boolean_transformation(old_column, new_column, table_ref)`
**Purpose:** Validates boolean column transformations during migration.

**Usage:**
```sql
{{ validate_boolean_transformation('"IsHidden"', 'is_hidden', 'stg_opendental__patient') }}
```

---

### `compare_model_outputs(old_model, new_model, key_column)`
**Purpose:** Compares outputs between old and new model versions during migration.

**Usage:**
```sql
{{ compare_model_outputs('stg_opendental__patient_old', 'stg_opendental__patient', 'patient_id') }}
```

---

## Implementation Guidelines

### For New Models
1. Always use `snake_case` for CTEs (following improved conventions)
2. Start with `staging_model_cte_structure()` template
3. Use `convert_opendental_boolean()` for all boolean fields
4. Use `clean_opendental_date()` for all date fields
5. Include `standardize_metadata_columns()` at the end

### For Model Migration
1. Create parallel model using new conventions
2. Use validation macros to compare outputs
3. Gradually migrate downstream dependencies
4. Remove old model after validation period

### Example New Staging Model
```sql
{{ config(
    materialized='table',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'patient') }}
),

renamed_columns as (
    select
        -- Primary key
        "PatNum" as patient_id,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"TxtMsgOk"') }} as text_messaging_consent,
        
        -- Date fields using macro
        {{ clean_opendental_date('"DateEntry"') }} as entry_date,
        {{ clean_opendental_date('"Birthdate"') }} as birth_date,
        
        -- String fields using macro  
        {{ clean_opendental_string('"CheckNum"') }} as check_number,
        
        -- Metadata using macro
        {{ standardize_metadata_columns() }}
    from source_data
)

select * from renamed_columns
```

## Benefits

1. **Consistency:** All models follow identical patterns
2. **Maintainability:** Changes to patterns only need macro updates
3. **Quality:** Built-in data cleaning and validation
4. **Documentation:** Self-documenting transformation logic
5. **Migration Safety:** Validation macros ensure data integrity
6. **Team Adoption:** Clear, reusable patterns for all developers

## Next Steps

1. Use these macros in all new model development
2. Create example migrations using validation macros
3. Update existing models gradually using the parallel development approach
4. Extend macros as new patterns emerge 