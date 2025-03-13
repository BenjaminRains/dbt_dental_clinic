# Understanding Staging Models in dbt

## Overview

Staging models represent the first transformation layer in dbt projects. They serve as the foundation for all downstream transformations by providing clean, standardized data from source systems.

## Purpose of Staging Models

Staging models typically:
1. Pull data directly from source tables
2. Clean and standardize the data
3. Rename fields to follow project conventions
4. Apply basic data quality rules
5. Add metadata columns

## Example: Payment Staging Model

Here's how our payment staging model works:

```sql
{{ config(
    materialized='incremental',
    unique_key='payment_id',
    persist_docs={'relation': false}
) }}

with source as (
    -- Pull from raw source table
    select * 
    from {{ source('opendental', 'payment') }}
    where PayDate >= DATE('2023-01-01')  
),

renamed as (
    select
        -- Primary key
        PayNum as payment_id,
        
        -- Standardize field names
        PayDate as payment_date,
        PayAmt as payment_amount,
        
        -- Add metadata
        current_timestamp() as _loaded_at
    from source
)
```

## Running Staging Models

### What Happens When You Run

When executing `dbt run --select stg_opendental__payment`, dbt:

1. **Compiles the SQL**
   - Processes all Jinja templating
   - Adds incremental logic
   - Stores compiled SQL in `target/compiled/`

2. **Executes the SQL**
   - Creates or updates the table in your data warehouse
   - For incremental models:
     - Only processes new/changed records
     - Uses unique key for record matching
     - Updates target table accordingly

3. **Tracks Results**
   - Logs success/failure in `target/run_results.json`
   - Records timing information
   - Updates manifest for dependency tracking

### Common Commands

```bash
# Run single staging model
dbt run --select stg_opendental__payment

# Run all staging models
dbt run --select staging

# Run model and dependencies
dbt run --select +stg_opendental__payment

# Full refresh (rebuild table)
dbt run --full-refresh --select stg_opendental__payment
```

## Configuration Options

Our staging models typically use these configurations:

```sql
{{ config(
    materialized='incremental',  # Only process new/changed records
    unique_key='payment_id',     # Unique identifier for records
    persist_docs={'relation': false}  # Don't persist docs to database
) }}
```

## Best Practices

1. **Naming Conventions**
   - Use `stg_` prefix
   - Include source system name
   - Include entity name
   - Example: `stg_opendental__payment`

2. **Field Standardization**
   - Rename fields to follow project conventions
   - Use clear, descriptive names
   - Document field meanings

3. **Data Quality**
   - Filter out invalid dates
   - Remove test/dummy data
   - Add appropriate metadata columns

4. **Performance**
   - Use incremental loading for large tables
   - Include appropriate date filters
   - Define clear unique keys

## Related Documentation

- [dbt Staging Documentation](https://docs.getdbt.com/docs/building-a-dbt-project/building-models)
- [Incremental Models](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/incremental-models)
- SQL Naming Conventions (link to your conventions doc)

---

*Last Updated: [Current Date]* 