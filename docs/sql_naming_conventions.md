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

## Implementation Notes

- When refactoring existing code, prioritize consistency within individual queries over immediate 
full compliance
- New code should adhere to these conventions from the start
- Comments should be used to clarify naming in complex cases

## Exceptions

In some special cases where direct SQL compatibility with external systems is required, these 
conventions may be modified. Such exceptions should be documented in the code.

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

---

*Document Version: 2.1 - Last Updated: March 26, 2025* 