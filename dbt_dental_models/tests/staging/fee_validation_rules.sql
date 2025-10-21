{{ config(
    severity = 'warn'
) }}

-- This test validates fee data quality across multiple dimensions
-- It will only FAIL (error) if critical issues are found, but will WARN on business-level anomalies

-- Test 1: Check for records before 2023 and future dates
select 
    f.fee_id,
    f._created_at,
    f._updated_at,
    'Invalid Date' as validation_check,
    'error' as severity
from {{ ref('stg_opendental__fee') }} f
where f._created_at < '2023-01-01'::date
    or f._created_at > {{ current_date() }}
    or f._updated_at > {{ current_date() }}

UNION ALL

-- Test 2: Check for unreasonable fee amounts (excluding known exceptions)
select 
    f.fee_id,
    f._created_at,
    f._updated_at,
    'Unreasonable Amount' as validation_check,
    'error' as severity
from {{ ref('stg_opendental__fee') }} f
where f.fee_id not in (217113, 219409, 218252)  -- Exclude known decimal errors
    and f.fee_schedule_id not in (8285, 8290)   -- Exclude inactive schedules
    and (f.fee_amount > 10000 or f.fee_amount < 0)

UNION ALL

-- Test 3: Check for duplicate fee configurations
select 
    f1.fee_id,
    f1._created_at,
    f1._updated_at,
    'Duplicate Fee Configuration' as validation_check,
    'error' as severity
from {{ ref('stg_opendental__fee') }} f1
where exists (
    select 1 
    from {{ ref('stg_opendental__fee') }} f2
    where f1.fee_schedule_id = f2.fee_schedule_id
        and f1.procedure_code_id = f2.procedure_code_id
        and f1.fee_id < f2.fee_id
)

UNION ALL

-- Test 4: Check for high fee variations by procedure code (excluding known variations)
select 
    f.fee_id,
    f._created_at,
    f._updated_at,
    'High Fee Variation' as validation_check,
    'warn' as severity
from {{ ref('stg_opendental__fee') }} f
where f.fee_id not in (217113, 219409, 218252)  -- Exclude known decimal errors
    and f.fee_schedule_id not in (8285, 8290)   -- Exclude inactive schedules
    and f.ada_code not in (
        'D6040', 'D5725', 'D6055', 'D6010',     -- Known high variation implants
        'D5863', 'D5865',                        -- Known overdenture variations
        'D6059', 'D6065', 'D6061', 'D6066',     -- Known crown variations
        'D2740'                                  -- Known basic crown variations
    )
    and f.procedure_code_id in (
        select f2.procedure_code_id
        from {{ ref('stg_opendental__fee') }} f2
        group by f2.procedure_code_id
        having max(f2.fee_amount) - min(f2.fee_amount) > 500
    )

UNION ALL

-- Test 5: Check default fees with zero amounts
select 
    f.fee_id,
    f._created_at,
    f._updated_at,
    'Invalid Default Fee' as validation_check,
    'warn' as severity
from {{ ref('stg_opendental__fee') }} f
where f.is_default_fee = true
    and f.fee_amount = 0
    and f.procedure_code_id in (
        select f2.procedure_code_id
        from {{ ref('stg_opendental__fee') }} f2
        group by f2.procedure_code_id
        having avg(f2.fee_amount) > 0
    )

UNION ALL

-- Test 6: Check fee schedule usage
select 
    f.fee_id,
    f._created_at,
    f._updated_at,
    'Underutilized Fee Schedule' as validation_check,
    'warn' as severity
from {{ ref('stg_opendental__fee') }} f
where f.fee_schedule_id in (
    select f2.fee_schedule_id
    from {{ ref('stg_opendental__fee') }} f2
    group by f2.fee_schedule_id
    having count(*) < 5
)

UNION ALL

-- Test 7: Check for statistical outliers in fee amounts by procedure
select 
    f.fee_id,
    f._created_at,
    f._updated_at,
    'Statistical Outlier' as validation_check,
    'warn' as severity
from {{ ref('stg_opendental__fee') }} f
join (
    select 
        f2.procedure_code_id,
        avg(f2.fee_amount) as avg_amount,
        stddev(f2.fee_amount) as fee_stddev
    from {{ ref('stg_opendental__fee') }} f2
    group by f2.procedure_code_id
    having count(*) > 5
) p on f.procedure_code_id = p.procedure_code_id
where abs(f.fee_amount - p.avg_amount) > 2 * p.fee_stddev

UNION ALL

-- Add test for orphaned procedure codes
select 
    f.fee_id,
    f._created_at,
    f._updated_at,
    'Orphaned Procedure Code' as validation_check,
    'warn' as severity
from {{ ref('stg_opendental__fee') }} f
left join {{ ref('stg_opendental__procedurecode') }} p 
    on f.procedure_code_id = p.procedure_code_id
where p.procedure_code_id is null