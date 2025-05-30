# SQL Naming Conventions

## Overview

This document outlines the standardized naming conventions for SQL code in the MDC Analytics 
project. This project uses dbt and Jinja2 templating, but with some custom conventions that 
differ from typical dbt practices - particularly in CTE naming.

Following these guidelines will ensure consistency, readability, and maintainability of 
our database queries, reports, and data extraction tools.

## General Principles

- **Readability**: Names should clearly indicate the purpose of the object
- **Consistency**: Similar objects should follow similar naming patterns
- **Precision**: Names should be specific enough to avoid ambiguity
- **Brevity**: Names should be concise while maintaining clarity

## Source Files and Staging Models

**Source System Convention**: 
- The OpenDental postgre source database uses CamelCase for column names (e.g., "PatNum", "AptDateTime", "ClaimNum")
- All identifier/key columns in source data use the `Num` suffix (e.g., "PatNum", "ClaimNum", "ProcNum")

**Source YAML Files**: 
- Document the actual raw database column names in their original CamelCase format
- Example: In `_sources/claims_sources.yml`, use "ClaimNum", "PatNum", "DateService"
- Maintain the `Num` suffix for all identifier columns as they appear in the source

**Staging Models**:
- Transform raw CamelCase column names to snake_case for downstream use
- Convert all identifier column suffixes from `_num` to `_id`
- Example: In `stg_opendental__claim.sql`:
  ```sql
  select
      "ClaimNum" as claim_id,        -- Transform from Num to _id
      "PatNum" as patient_id,        -- Transform from Num to _id
      "ProcNum" as procedure_id,     -- Transform from Num to _id
      "DateService" as service_date
  ```
- Note: When using incremental models, the `unique_key` config must reference the source column name:
  ```sql
  {{ config(
      materialized='incremental',
      unique_key='claim_num'  -- Use source column name (with _num)
  ) }}
  ```

**Staging Model YAML Files**:
- Use snake_case column names that match the output of the staging models (not the source database)
- Use `_id` suffix for all identifier columns (not `_num`)
- Example: In `_stg_opendental__adjustment.yml`, use "adjustment_id", "patient_id", "procedure_id"
- These document the transformed column names that downstream models will reference

This dual-convention pattern allows us to accurately document both the source system and our transformed data models while maintaining consistent naming within each context.

## Naming Conventions

### Database Column References

**Rule**: Use CamelCase for all references to raw database columns.

**Rationale**: This maintains consistency with the actual database schema, reducing the risk of 
errors when referencing database fields.

**Examples**:
- `DatePay` - Payment date column from database
- `PatNum` - Patient number primary key
- `PayType` - Payment type identifier
- `SplitAmt` - Payment split amount

### Derived/Calculated Fields

**Rule**: Use snake_case for all derived or calculated fields.

**Rationale**: This visually distinguishes derived data from raw database columns, making it 
immediately clear which fields are calculated versus direct database references.

**Examples**:
- `total_payment` - Sum of multiple payment fields
- `percent_current` - Calculated percentage of current payments
- `days_since_payment` - Calculated number of days
- `average_payment_amount` - Average of payment amounts

### Common Table Expression (CTE) Names

**Rule**: Use CamelCase for ALL CTEs in SQL, including those that might typically use snake_case in other dbt projects.

**Rationale**: While this differs from typical dbt conventions, our project standardizes on CamelCase for CTEs to maintain consistency with our database entity naming and to clearly distinguish CTEs from variables/attributes.

**Examples**:
- `Source` - Raw source data CTE
- `Renamed` - Cleaned and renamed columns CTE
- `PaymentTypeDef` - Payment type definitions CTE
- `PatientBalances` - Patient balance information CTE
- `UnearnedTypeDefinition` - Unearned income type definitions
- `AllPaymentTypes` - Aggregated payment types

**Note**: This is an intentional deviation from typical dbt practices where CTEs often use snake_case. Our project prioritizes consistency with our database naming conventions.

### SQL File Names

**Rule**: Use snake_case for all SQL file names.

**Rationale**: This follows Pythonic conventions for file naming, making files easier to work with 
in the Python environment.

**Examples**:
- `unearned_income_payment_type.sql` - SQL file containing payment type queries
- `payment_split_analysis.sql` - SQL file for split analysis
- `monthly_trend_report.sql` - Monthly trend report queries

## dbt-Specific Conventions

**Note**: This project uses dbt and Jinja2 templating for SQL transformations. While we follow most dbt conventions, our CTE naming (as noted above) intentionally differs from typical dbt practices.

### Model Names and Other dbt Elements

**Rule**: Use snake_case for all dbt model names, following the pattern:
- Staging: `stg_[source]__[entity]`
- Intermediate: `int_[entity]_[verb]`
- Marts: `[mart]_[entity]`

**Examples**:
- `stg_opendental__payment`
- `int_payments_combined`
- `fct_daily_payments`
- `dim_payment_types`

### Jinja and Macros

**Rule**: Use snake_case for Jinja variables and macro names.

**Examples**:
```sql
{% set payment_types = ['cash', 'credit', 'check'] %}

{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

{% macro get_payment_types() %}
    ...
{% endmacro %}
```

### Reference Syntax

**Rule**: Use snake_case within dbt reference functions.

**Examples**:
```sql
select * from {{ ref('stg_opendental__payment') }}
select * from {{ source('opendental', 'payment') }}
```

## Mart Model Nomenclature

### Mart Model Types

**Rule**: Use consistent prefixes for different types of mart models:
- `mart_` prefix for aggregated business metrics
- `fct_` prefix for fact tables
- `dim_` prefix for dimension tables

**Examples**:
- `mart_financial_performance` - Aggregated financial metrics
- `fct_procedure` - Procedure-level fact table
- `dim_date` - Date dimension table

### Mart Model Documentation

**Rule**: Document mart models in YAML files with:
- Clear descriptions of the mart's purpose
- Comprehensive column documentation
- Relationship tests to dimension tables
- Business metric definitions

**Example Structure**:
```yaml
models:
  - name: mart_financial_performance
    description: "Mart model for comprehensive financial performance analysis"
    columns:
      - name: date_id
        description: "Foreign key to dim_date"
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_id
```

### Mart Model Metrics

**Rule**: Use clear, business-focused names for metrics:
- Use snake_case for metric names
- Include units in the name where applicable
- Use descriptive prefixes for time-based metrics

**Examples**:
- `total_revenue` - Sum of all revenue
- `procedure_count` - Count of procedures
- `average_fee` - Average fee per procedure
- `collection_rate` - Percentage of fees collected
- `ar_aging_0_30` - Accounts receivable aging 0-30 days

### Fact Table Conventions

**Rule**: Fact tables should:
- Include all relevant dimension keys
- Use consistent naming for common fields
- Include metadata columns for tracking

**Example Structure**:
```yaml
models:
  - name: fct_procedure
    description: "Fact table containing procedure execution details and metrics"
    columns:
      - name: procedure_id
        description: "Primary key for the procedure fact"
        tests:
          - unique
          - not_null
      - name: _created_at
        description: "When the procedure record was created"
      - name: _updated_at
        description: "When the procedure record was last updated"
```

## Visual Differentiation Benefits

This multi-case approach provides immediate visual cues about the nature of each element:

```sql
-- Standard pattern in our dbt project
with Source as (
    select * from {{ ref('stg_opendental__payment') }}
),

Renamed as (
    select
        PayNum as payment_id,        -- Raw DB column to derived
        DatePay as payment_date,     -- Raw DB column to derived
        amount * 100 as amount_cents -- Calculated field
    from Source
),

-- Business logic CTE
PaymentSummary as (
    select
        payment_date,
        sum(amount_cents) as daily_total
    from Renamed
    group by payment_date
)
```

## Data Type Conversions

### Boolean Conversions

**Source System Convention**: OpenDental often uses smallint (0/1) for boolean flags, but PostgreSQL requires explicit conversion to boolean.

**Rule**: Use CASE statement for smallint to boolean conversions, not direct casting.

**Wrong**:
```sql
"IsHidden"::boolean as is_hidden,         -- Will fail
cast("IsHidden" as boolean) as is_hidden  -- Will fail
```

**Correct**:
```sql
CASE 
    WHEN "IsHidden" = 1 THEN true
    WHEN "IsHidden" = 0 THEN false
    ELSE null 
END as is_hidden
```

**Rationale**: PostgreSQL does not support direct casting from smallint to boolean. The CASE statement 
explicitly handles the conversion and also provides clarity about the meaning of the values.

**Common Examples**:
- `IsHidden` → `is_hidden`
- `IsActive` → `is_active`
- `IsDeleted` → `is_deleted`
- `IsSigned` → `is_signed`

This pattern should be used consistently across all staging models where boolean flags are stored as smallint in the source data.

## Metadata Columns

**Rule**: Metadata columns are added at different stages of the data pipeline. Understanding when and where each metadata column is added is crucial for proper data lineage and change tracking.

### ETL Stage (Initial Data Load)
When data is first loaded from OpenDental (MySQL) to PostgreSQL via the ETL pipeline:

1. **Only `_loaded_at` is added**:
   - Added by the ETL job (`mysql_postgre_incremental.py`)
   - Tracks when data was loaded into PostgreSQL
   - Uses `current_timestamp` at time of load
   - Example:
     ```sql
     SELECT 
         *,
         current_timestamp as _loaded_at
     FROM source_table
     ```

2. **Source timestamps are preserved**:
   - Original creation/update timestamps from OpenDental are kept as-is
   - These will be transformed in the staging models
   - Example source columns:
     - `DateEntry`, `SecDateTEntry` (creation timestamps)
     - `DateTStamp`, `SecDateTEdit` (update timestamps)

### Staging Model Stage
When dbt staging models transform the data:

1. **Required Metadata Columns**:
   - `_loaded_at`: Preserved from ETL stage
   - `_created_at`: Transformed from source creation timestamp
   - `_updated_at`: Transformed from source update timestamp

2. **Transformation Example**:
   ```sql
   select
       -- Source columns
       "PatNum" as patient_id,
       
       -- Metadata columns
       _loaded_at,  -- Preserved from ETL
       "DateEntry" as _created_at,  -- Transformed from source
       coalesce("DateTStamp", "DateEntry") as _updated_at  -- Transformed from source
   from {{ source('opendental', 'patient') }}
   ```

### Metadata Column Rules

1. **ETL Stage**:
   - Only add `_loaded_at`
   - Use `current_timestamp`
   - Preserve all source timestamps

2. **Staging Stage**:
   - Transform source timestamps to standard metadata columns
   - Common source columns to transform:
     - `_created_at` from:
       - `DateEntry` → `_created_at`
       - `SecDateTEntry` → `_created_at`
       - `CreatedDate` → `_created_at`
       - `InsertDate` → `_created_at`
     - `_updated_at` from:
       - `DateTStamp` → `_updated_at`
       - `SecDateTEdit` → `_updated_at`
       - `ModifiedDate` → `_updated_at`

3. **Naming Convention**:
   - All metadata columns use underscore prefix (`_`)
   - Use snake_case
   - Be consistent with column names across all models

4. **Documentation**:
   - Document source timestamp mappings in staging model YAML files
   - Include metadata columns in model documentation
   - Example YAML:
     ```yaml
     models:
       - name: stg_opendental__patient
         columns:
           - name: _loaded_at
             description: "When the data was loaded into PostgreSQL by ETL"
           - name: _created_at
             description: "Transformed from DateEntry - when record was created in OpenDental"
           - name: _updated_at
             description: "Transformed from DateTStamp - when record was last updated in OpenDental"
     ```

### Implementation Notes

1. **ETL Pipeline**:
   - Keep ETL metadata minimal (`_loaded_at` only)
   - Preserve all source timestamps
   - Document timestamp mappings for staging models

2. **Staging Models**:
   - Transform source timestamps to standard metadata columns
   - Include all three required metadata columns
   - Document transformations in YAML files

3. **Incremental Models**:
   - Include `_updated_at` in unique_key configuration
   - Use `_updated_at` for incremental logic
   - Example:
     ```sql
     {{ config(
         materialized='incremental',
         unique_key='patient_id',
         incremental_strategy='merge',
         merge_update_columns=['_updated_at', '_loaded_at']
     ) }}
     ```

## Implementation Notes

- When refactoring existing code, prioritize consistency within individual queries over immediate 
full compliance
- New code should adhere to these conventions from the start
- Comments should be used to clarify naming in complex cases

## Exceptions

In some special cases where direct SQL compatibility with external systems is required, these 
conventions may be modified. Such exceptions should be documented in the code.

---

*Document Version: 2.1 - Last Updated: March 26, 2025* 