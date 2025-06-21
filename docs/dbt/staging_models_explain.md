# Understanding Staging Models in dbt

## Overview

Staging models represent the first transformation layer in dbt projects. They serve as the 
foundation for all downstream transformations by providing clean, standardized data from 
source systems.

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

## Complete List of Staging Models (needs updated)

### Incremental Models
1. stg_opendental__adjustment (4,410 records)
2. stg_opendental__appointment (19,240 records)
3. stg_opendental__claim (10,272 records)
4. stg_opendental__claimpayment (7,494 records)
5. stg_opendental__claimproc (67,054 records)
6. stg_opendental__claimtracking (13,872 records)
7. stg_opendental__commlog (126,284 records)
8. stg_opendental__fee (2,568 records)
9. stg_opendental__feesched (9 records)
10. stg_opendental__histappointment (253,778 records)
11. stg_opendental__labcase (1,789 records)
12. stg_opendental__payment (11,766 records)
13. stg_opendental__paysplit (51,539 records)
14. stg_opendental__perioexam (2,353 records)
15. stg_opendental__periomeasure (129,418 records)
16. stg_opendental__procedurecode (815 records)
17. stg_opendental__procedurelog (88,759 records)
18. stg_opendental__procgroupitem (24,749 records)
19. stg_opendental__procmultivisit (6 records)
20. stg_opendental__procnote (35,545 records)
21. stg_opendental__proctp (892 records)
22. stg_opendental__recall (5,369 records)
23. stg_opendental__schedule (14,822 records)
24. stg_opendental__sheetfield (1,400,795 records)
25. stg_opendental__statement (10,230 records)
26. stg_opendental__treatplan (250 records)
27. stg_opendental__treatplanattach (55,180 records)
28. stg_opendental__treatplanparam (3 records)
29. stg_opendental__scheduleop (29,738 records)
30. stg_opendental__statementprod (39,284 records)

### Table Models
1. stg_opendental__patient (32,700 records)

### View Models
1. stg_opendental__allergy
2. stg_opendental__allergydef
3. stg_opendental__appointmenttype
4. stg_opendental__autocode
5. stg_opendental__benefit
6. stg_opendental__carrier
7. stg_opendental__codegroup
8. stg_opendental__definition
9. stg_opendental__disease
10. stg_opendental__diseasedef
11. stg_opendental__document
12. stg_opendental__employee
13. stg_opendental__insbluebook
14. stg_opendental__insbluebooklog
15. stg_opendental__insplan
16. stg_opendental__inssub
17. stg_opendental__insverify
18. stg_opendental__insverifyhist
19. stg_opendental__patientlink
20. stg_opendental__patientnote
21. stg_opendental__patplan
22. stg_opendental__pharmacy
23. stg_opendental__pharmclinic
24. stg_opendental__program
25. stg_opendental__programproperty
26. stg_opendental__provider
27. stg_opendental__recalltrigger
28. stg_opendental__recalltype
29. stg_opendental__refattach
30. stg_opendental__referral
31. stg_opendental__rxdef
32. stg_opendental__rxnorm
33. stg_opendental__rxpat
34. stg_opendental__sheet
35. stg_opendental__sheetdef
36. stg_opendental__sheetfielddef
37. stg_opendental__task
38. stg_opendental__timeadjust
39. stg_opendental__toothinitial
40. stg_opendental__usergroup
41. stg_opendental__usergroupattach
42. stg_opendental__userod
43. stg_opendental__zipcode

### Summary Statistics
- Total Models: 74
  - Incremental Models: 30
  - View Models: 43
  - Table Models: 1
- Largest Tables (by record count):
  1. sheetfield (1,400,795 records)
  2. histappointment (253,778 records)
  3. periomeasure (129,418 records)
  4. commlog (126,284 records)
  5. procedurelog (88,759 records)

---

*Last Updated: [Current Date]* 