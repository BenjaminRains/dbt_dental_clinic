-- Test 1: Check for records before 2023 and future dates
select 
    fee_id,
    created_at,
    updated_at,
    'Invalid Date' as validation_check
from {{ ref('stg_opendental__fee') }}
where created_at < '2023-01-01'::date
    or created_at > {{ current_date() }}
    or updated_at > {{ current_date() }}

UNION ALL

-- Test 2: Check for unreasonable fee amounts
select 
    fee_id,
    created_at,
    updated_at,
    'Unreasonable Amount' as validation_check
from {{ ref('stg_opendental__fee') }}
where fee_amount > 10000
    or fee_amount < 0

UNION ALL

-- Test 3: Check for duplicate fee configurations
select 
    f1.fee_id,
    f1.created_at,
    f1.updated_at,
    'Duplicate Fee Configuration' as validation_check
from {{ ref('stg_opendental__fee') }} f1
where exists (
    select 1 
    from {{ ref('stg_opendental__fee') }} f2
    where f1.fee_schedule_id = f2.fee_schedule_id
        and f1.procedure_code_id = f2.procedure_code_id
        and f1.fee_id < f2.fee_id
)

UNION ALL

-- Test 4: Check for high fee variations by procedure code
select 
    f.fee_id,
    f.created_at,
    f.updated_at,
    'High Fee Variation' as validation_check
from {{ ref('stg_opendental__fee') }} f
where f.procedure_code_id in (
    select 
        procedure_code_id
    from {{ ref('stg_opendental__fee') }}
    group by procedure_code_id
    having max(fee_amount) - min(fee_amount) > 500
)

UNION ALL

-- Test 5: Check default fees with zero amounts
select 
    fee_id,
    created_at,
    updated_at,
    'Invalid Default Fee' as validation_check
from {{ ref('stg_opendental__fee') }}
where is_default_fee = 1
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
    created_at,
    updated_at,
    'Underutilized Fee Schedule' as validation_check
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
    f.created_at,
    f.updated_at,
    'Statistical Outlier' as validation_check
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