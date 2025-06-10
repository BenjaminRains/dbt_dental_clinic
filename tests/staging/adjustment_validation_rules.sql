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
    where 
        -- Check for invalid procedure flag combinations
        (procedure_id > 0 AND is_procedure_adjustment = false)
        OR (procedure_id = 0 AND is_procedure_adjustment = true)
        -- Only require documentation for specific cases
        OR (
            procedure_id = 0 
            AND (adjustment_note IS NULL OR trim(adjustment_note) = '')
            AND (
                -- Require documentation for insurance writeoffs
                adjustment_type_id = 188
                -- Require documentation for large adjustments
                OR adjustment_amount <= -1000
                -- Require documentation for reallocations
                OR adjustment_type_id = 235
            )
        )

    UNION ALL

    -- Test 6: Retroactive adjustment validation
    select distinct
        adjustment_id,
        'Invalid retroactive adjustment flag' as failure_reason
    from staging_data
    where (procedure_date != adjustment_date and is_retroactive_adjustment = false)
        or (procedure_date = adjustment_date and is_retroactive_adjustment = true)

    UNION ALL

    -- Test 8: Basic amount validation (simplified from complex business logic)
    select distinct
        adjustment_id,
        'Invalid amount/direction consistency' as failure_reason
    from staging_data
    where (adjustment_amount > 0 and adjustment_direction != 'positive')
        or (adjustment_amount < 0 and adjustment_direction != 'negative')
        or (adjustment_amount = 0 and adjustment_direction != 'zero')

    UNION ALL

    -- Test 1: procedure_id and is_procedure_adjustment consistency
    select
        adjustment_id,
        'Invalid procedure_id and is_procedure_adjustment relationship' as failure_reason
    from staging_data
    where not (
        (procedure_id > 0 and is_procedure_adjustment = true) or
        (procedure_id = 0 and is_procedure_adjustment = false)
    )

    UNION ALL

    -- Test 2: adjustment_amount validation
    select
        adjustment_id,
        'Invalid adjustment amount for procedure adjustment' as failure_reason
    from staging_data
    where adjustment_amount = 0
        and is_procedure_adjustment = true
        and procedure_id > 0

    UNION ALL

    -- Test 3: date validation
    select
        adjustment_id,
        'Invalid adjustment date' as failure_reason
    from staging_data
    where adjustment_date < '2023-01-01'::date
        or adjustment_date > current_date
),
detailed_failures as (
    select 
        v.adjustment_id,
        v.failure_reason,
        s.adjustment_date,
        s.adjustment_amount,
        s.procedure_id,
        s.is_procedure_adjustment,
        s.adjustment_type_id,
        s.adjustment_note,
        s.provider_id,
        s.patient_id,
        s.is_retroactive_adjustment,
        s.adjustment_direction
    from validation_failures v
    join staging_data s using (adjustment_id)
)
select * from detailed_failures
order by 
    failure_reason,
    adjustment_date desc

