-- Test 1: Check for records before 2023 and future dates
select 
    fee_id,
    date_created,
    date_updated,
    'Invalid Date' as validation_check,
    'error' as severity
from {{ ref('stg_opendental__fee') }}
where date_created < '2023-01-01'::date
    or date_created > {{ current_date() }}
    or date_updated > {{ current_date() }}

UNION ALL

-- Test 2: Check for unreasonable fee amounts (excluding known exceptions)
select 
    fee_id,
    date_created,
    date_updated,
    'Unreasonable Amount' as validation_check,
    'error' as severity
from {{ ref('stg_opendental__fee') }}
where fee_id not in (217113, 219409, 218252)  -- Exclude known decimal errors
    and fee_schedule_id not in (8285, 8290)   -- Exclude inactive schedules
    and (fee_amount > 10000 or fee_amount < 0)

UNION ALL

-- Test 3: Check for duplicate fee configurations
select 
    f1.fee_id,
    f1.date_created,
    f1.date_updated,
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
    f.date_created,
    f.date_updated,
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
        select procedure_code_id
        from {{ ref('stg_opendental__fee') }}
        group by procedure_code_id
        having max(fee_amount) - min(fee_amount) > 500
    )

UNION ALL

-- Test 5: Check default fees with zero amounts
select 
    fee_id,
    date_created,
    date_updated,
    'Invalid Default Fee' as validation_check,
    'warn' as severity
from {{ ref('stg_opendental__fee') }}
where is_default_fee = true
    and fee_amount = 0
    and procedure_code_id in (
        select f2.procedure_code_id
        from {{ ref('stg_opendental__fee') }} f2
        group by f2.procedure_code_id
        having avg(f2.fee_amount) > 0
    )

UNION ALL

-- Test 6: Check fee schedule usage
select 
    fee_id,
    date_created,
    date_updated,
    'Underutilized Fee Schedule' as validation_check,
    'warn' as severity
from {{ ref('stg_opendental__fee') }}
where fee_schedule_id in (
    select fee_schedule_id
    from {{ ref('stg_opendental__fee') }}
    group by fee_schedule_id
    having count(*) < 5
)

UNION ALL

-- Test 7: Check for statistical outliers in fee amounts by procedure
select 
    f.fee_id,
    f.date_created,
    f.date_updated,
    'Statistical Outlier' as validation_check,
    'warn' as severity
from {{ ref('stg_opendental__fee') }} f
join (
    select 
        procedure_code_id,
        avg(fee_amount) as avg_amount,
        stddev(fee_amount) as fee_stddev
    from {{ ref('stg_opendental__fee') }}
    group by procedure_code_id
    having count(*) > 5
) p on f.procedure_code_id = p.procedure_code_id
where abs(f.fee_amount - p.avg_amount) > 2 * p.fee_stddev

UNION ALL

-- Add test for orphaned procedure codes
select 
    fee_id,
    date_created,
    date_updated,
    'Orphaned Procedure Code' as validation_check,
    'warn' as severity
from {{ ref('stg_opendental__fee') }} f
left join {{ ref('stg_opendental__procedurecode') }} p 
    on f.procedure_code_id = p.procedure_code_id
where p.procedure_code_id is null