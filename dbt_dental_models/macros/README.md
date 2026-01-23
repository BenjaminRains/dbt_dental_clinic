# dbt Macros Directory

This directory contains utility macros and generic test macros for the dbt dental clinic project. All macros are organized by purpose for easy discovery and maintenance.

## Directory Structure

```
macros/
├── tests/              # Generic test macros (reusable across models)
│   ├── core/           # Core wrapper tests for quoted column names
│   ├── data_quality/   # Generic data quality validation tests
│   ├── domain/         # Business domain-specific tests
│   └── fee/            # Fee-specific validation tests
├── utils/              # Utility macros for transformations and helpers
└── README.md           # This file
```

### Organization Principles

- **`tests/`**: All generic test macros (called via `tests:` in YAML files)
  - **`core/`**: Wrapper tests that handle quoted column names from OpenDental
  - **`data_quality/`**: Reusable tests for common data validation patterns
  - **`domain/`**: Business-specific tests for dental clinic operations
  - **`fee/`**: Specialized tests for fee validation and analysis
  
- **`utils/`**: Utility macros for data transformations and helpers
  - Data cleaning functions (dates, booleans, strings)
  - Metadata standardization
  - ID transformations
  - Tracking and validation helpers
  - Schema naming configuration

---

## Utility Macros (`utils/`)

These macros implement improved naming conventions and standardize common transformation patterns across all dbt models.

### Schema Configuration

#### `generate_schema_name(custom_schema_name, node)`
**Purpose:** Overrides dbt's default schema naming behavior to use custom schema names directly without prefixing.

**How it works:**
- By default, dbt prefixes custom schema names with the target schema from `profiles.yml`
- Example: If `target.schema = 'raw'` and a model specifies `schema: 'staging'`, dbt would create `raw_staging`
- This macro overrides that to create schemas named exactly `staging`, `int`, `marts`

**Usage:**
This macro is **automatically called by dbt** for all models. You don't need to explicitly call it.

**Configuration:**
Schema names are configured in `dbt_project.yml`:
```yaml
models:
  dbt_dental_models:
    staging:
      +schema: staging    # Creates schema: staging (not raw_staging)
    intermediate:
      +schema: int        # Creates schema: int (not raw_int)
    marts:
      +schema: marts      # Creates schema: marts (not raw_marts)
```

**Result:**
- Models in `models/staging/` → `staging` schema
- Models in `models/intermediate/` → `int` schema
- Models in `models/marts/` → `marts` schema

**Why this matters:**
- Keeps schema names clean and predictable
- Matches intended architecture regardless of target schema in profiles.yml
- Prevents schema name confusion (e.g., `raw_staging` vs `staging`)

**When to keep this macro (current approach):**
✅ **Single environment per database** - Each environment (dev, demo, clinic) uses its own database
✅ **Schema-based multi-tenancy** - Tenants are handled via separate schemas (e.g., `mdc`, `glic`) within the same database, not via schema prefixes
✅ **Consistent naming** - You want the same schema names (`staging`, `int`, `marts`) regardless of which target/profile is used
✅ **Clean architecture** - Schema names reflect logical layers, not deployment environments

**When to remove this macro (use default dbt behavior):**
⚠️ **Multiple environments in same database** - If you need `dev_staging`, `prod_staging`, `demo_staging` in the same database
⚠️ **Developer isolation** - Multiple developers working in the same database need separate schemas (e.g., `dev_alice_staging`, `dev_bob_staging`)
⚠️ **Environment-based isolation** - When `target.schema` in profiles.yml represents a meaningful environment identifier that should prefix all schemas
⚠️ **CI/CD parallel runs** - When running multiple dbt jobs in parallel against the same database and need automatic schema isolation

**Current project approach:**
This project uses **separate databases per environment** (dev, demo, clinic) with schema-based multi-tenancy for clinics (MDC, GLIC). Therefore, the macro is appropriate - we want clean schema names (`staging`, `int`, `marts`) that are consistent across environments, with tenant isolation handled at the schema level (`mdc`, `glic`) rather than via prefixes.

**See also:** `dbt_project.yml` for schema configuration

---

### Core Transformation Macros

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

**Output:** Adds two columns: `_loaded_at`, '_transformed_at'

---



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

## Test Macros (`tests/`)

These are generic (reusable) test macros that can be applied to any model via YAML configuration.

### Core Tests (`tests/core/`)

Wrapper tests that handle OpenDental's quoted column names:

- **`test_accepted_values_quoted`**: Validates column values against an allowed list (quoted columns)
- **`test_expression_is_true_quoted`**: Tests a boolean expression (quoted columns)
- **`test_not_null_quoted`**: Validates non-null values (quoted columns)
- **`test_unique_quoted`**: Validates uniqueness (quoted columns)

**Usage Example:**
```yaml
models:
  - name: stg_opendental__patient
    columns:
      - name: patient_status
        tests:
          - accepted_values_quoted:
              column_name: PatientStatus
              values: [0, 1, 2, 3]
```

### Data Quality Tests (`tests/data_quality/`)

Generic validation tests for common data patterns:

- **`boolean_values`**: Validates boolean columns contain only true/false/null
- **`date_range_test`**: Validates dates fall within expected range
- **`is_boolean_or_null`**: Validates field is boolean or null
- **`non_negative_or_null`**: Validates numeric values are non-negative or null
- **`valid_tooth_number`**: Validates dental tooth numbering (1-32)
- **`valid_tooth_surface`**: Validates tooth surface codes
- **`test_column_greater_than`**: Validates one column is greater than another
- **`test_date_after`**: Validates one date is after another
- **`test_date_not_future`**: Validates date is not in the future
- **`test_timestamp_not_future`**: Validates timestamp is not in the future
- **`test_string_length`**: Validates string length constraints
- **`test_string_not_empty`**: Validates strings are not empty
- **`test_status_with_date`**: Validates status fields have corresponding dates
- **`test_has_non_zero_balance`**: Validates balance fields are non-zero when expected
- **`relationships_with_source_filter`**: Tests relationships with filtered source data
- **`test_relationships_with_third_table`**: Tests multi-table relationships

**Usage Example:**
```yaml
models:
  - name: stg_opendental__patient
    columns:
      - name: birthdate
        tests:
          - date_range_test:
              min_date: "'1900-01-01'"
              max_date: "current_date"
```

### Domain Tests (`tests/domain/`)

Business logic validation specific to dental clinic operations:

- **`payment_validation_rules_generic`**: Validates payment business rules (generic version)
- **`paysplit_validation_rules_generic`**: Validates payment split rules (generic version)
- **`insurance_validation_test`**: Validates insurance data integrity
- **`collection_efficiency_test`**: Validates collection efficiency metrics
- **`procedurecode_flag_validation`**: Validates procedure code flags
- **`procedurecode_format_validation`**: Validates procedure code formats
- **`procedurecode_prefix_validation`**: Validates procedure code prefixes
- **`test_insurance_payment_estimate`**: Validates insurance estimates
- **`test_insurance_verification`**: Validates insurance verification data
- **`test_no_problematic_income_transfers`**: Detects problematic income transfers
- **`test_payment_balance_alignment`**: Validates payment and balance alignment
- **`test_transfer_balance_validation`**: Validates transfer balances
- **`histappointment_chronology`**: Validates appointment history order
- **`histappointment_missing_appointments`**: Detects missing historical appointments
- **`warn_new_procedure_codes`**: Warns about new procedure codes

**Usage Example:**
```yaml
models:
  - name: stg_opendental__payment
    tests:
      - payment_validation_rules
```

### Fee Tests (`tests/fee/`)

Specialized validation for fee schedules and amounts:

- **`test_fee_default_zero`**: Detects default fees incorrectly set to zero
- **`test_fee_relative_amount`**: Validates fees relative to other schedules
- **`test_fee_schedule_usage`**: Validates fee schedule usage patterns
- **`test_fee_statistical_outlier`**: Detects statistical outliers in fee amounts
- **`test_fee_variation`**: Validates fee variation within expected ranges
- **`test_fee_variation_exclusions`**: Validates fee variation with exclusions
- **`test_income_transfer_analysis`**: Analyzes income transfer patterns
- **`test_suspicious_fee_amounts`**: Detects suspicious fee amounts

**Usage Example:**
```yaml
models:
  - name: int_fee_analysis
    tests:
      - fee_default_zero
      - fee_statistical_outlier
```

---

## Implementation Guidelines

### For New Models
1. Always use `snake_case` for CTEs (following improved conventions)
2. Start with `staging_model_cte_structure()` template (from `utils/`)
3. Use `convert_opendental_boolean()` for all boolean fields (from `utils/`)
4. Use `clean_opendental_date()` for all date fields (from `utils/`)
5. Include `standardize_metadata_columns()` at the end (from `utils/`)
6. Add appropriate generic tests from `tests/` in YAML files

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
3. **Quality:** Built-in data cleaning and validation through utility macros and tests
4. **Documentation:** Self-documenting transformation logic
5. **Migration Safety:** Validation macros ensure data integrity
6. **Team Adoption:** Clear, reusable patterns for all developers
7. **Organization:** Clear separation between utilities (`utils/`) and tests (`tests/`)
8. **Discoverability:** Logical grouping makes it easy to find the right macro or test

## Finding the Right Macro

- **Need to transform data?** → Look in `utils/`
- **Need to validate data?** → Look in `tests/`
  - General validation? → `tests/data_quality/`
  - Business rules? → `tests/domain/`
  - Fee-specific? → `tests/fee/`
  - Quoted column wrappers? → `tests/core/`

## Next Steps

1. Use utility macros from `utils/` in all new model development
2. Apply appropriate generic tests from `tests/` in model YAML files
3. Create example migrations using validation macros
4. Update existing models gradually using the parallel development approach
5. Extend macros as new patterns emerge, placing them in appropriate directories 