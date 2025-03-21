/*
ADJUSTMENT VALIDATION RULES
===========================

Purpose: Validates adjustment data patterns and business rules discovered during analysis. This file
references the actual source table file 'stg_opendental__adjustment'.

Key Validation Rules:
*/

with staging_data as (
    -- Get the filtered staging data once
    select * from {{ ref('stg_opendental__adjustment') }}
),
validation_failures as (
    -- Test 1: Primary key uniqueness
    select
        adjustment_id,
        'Test 1: Duplicate adjustment_id found' as failure_reason
    from staging_data
    group by adjustment_id
    having count(*) > 1

    UNION ALL

    -- Test 3: Foreign key relationships and required fields
    select distinct
        adjustment_id,
        'Invalid foreign key or required field' as failure_reason
    from staging_data
    where (patient_id <= 0 or patient_id is null)
        or (provider_id < 0)
        or (procedure_id < 0)
        or provider_id is null
        or provider_id = 0
        or provider_id not in (28, 47, 52, 48, 20, 19, 29, 53, 7, 30, 50, 43, 3, 12)

    UNION ALL

    -- Test 4: Adjustment amount logic
    select distinct
        adjustment_id,
        'Invalid adjustment amount direction' as failure_reason
    from staging_data
    where (adjustment_amount = 0 and adjustment_direction != 'zero')
        or (adjustment_amount > 0 and adjustment_direction != 'positive')
        or (adjustment_amount < 0 and adjustment_direction != 'negative')

    UNION ALL

    -- Test 5: Procedure flags and documentation
    select distinct
        adjustment_id,
        'Invalid procedure flags or missing documentation' as failure_reason
    from staging_data
    where (procedure_id is not null and is_procedure_adjustment = 0)
        or (procedure_id is null and is_procedure_adjustment = 1)
        or (procedure_id is null and (adjustment_note is null or trim(adjustment_note) = ''))

    UNION ALL

    -- Test 6: Retroactive adjustment validation
    select distinct
        adjustment_id,
        'Invalid retroactive adjustment flag' as failure_reason
    from staging_data
    where (procedure_date != adjustment_date and is_retroactive_adjustment = 0)
        or (procedure_date = adjustment_date and is_retroactive_adjustment = 1)

    UNION ALL

    -- Test 8: Employee and provider discount validations
    select distinct
        adjustment_id,
        'Invalid discount flags' as failure_reason
    from staging_data
    where (adjustment_type_id in (472, 485, 655) and is_employee_discount = 0)
        or (adjustment_type_id not in (472, 485, 655) and is_employee_discount = 1)
        or (adjustment_type_id in (474, 475, 601) and is_provider_discount = 0)
        or (adjustment_type_id not in (474, 475, 601) and is_provider_discount = 1)

    UNION ALL

    -- Test 9: Category mapping validation
    select distinct
        adjustment_id,
        'Invalid category mapping' as failure_reason
    from staging_data
    where adjustment_type_id = 188 and adjustment_category != 'insurance_writeoff'
        or adjustment_type_id = 474 and adjustment_category != 'provider_discount'
        or adjustment_type_id = 186 and adjustment_category != 'senior_discount'
),
large_adjustment_warnings as (
    select 
        adjustment_id,
        'WARNING: Large adjustment' as message_type,
        adjustment_date,
        adjustment_amount,
        provider_id,
        procedure_id,
        adjustment_type_id,
        case 
            when abs(adjustment_amount) >= 5000 then 'CRITICAL'
            when abs(adjustment_amount) >= 2500 then 'HIGH'
            else 'MEDIUM'
        end as warning_level
    from staging_data
    where abs(adjustment_amount) >= 1000
),
combined_results as (
    -- Validation Failures
    select 
        adjustment_id,
        'FAIL' as message_type,
        failure_reason as description,
        null as warning_level,
        adjustment_date,
        adjustment_amount,
        provider_id,
        adjustment_type_id
    from validation_failures v
    join staging_data a using (adjustment_id)
    
    UNION ALL
    
    -- Warnings
    select 
        adjustment_id,
        message_type,
        'Large adjustment detected' as description,
        warning_level,
        adjustment_date,
        adjustment_amount,
        provider_id,
        adjustment_type_id
    from large_adjustment_warnings
)
select * from combined_results
order by 
    case when message_type = 'FAIL' then 1 else 2 end,
    abs(adjustment_amount) desc

