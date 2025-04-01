/*
Detailed Analysis of Fee Validation Issues:
1. Unreasonable Amount (3 records) - Fees > $10,000 or < 0
2. High Fee Variation (78 records) - Variation > $500 between min/max
3. Underutilized Fee Schedule (3 records) - Schedules with < 5 fees
*/

-- 1. Unreasonable Amount Records (3)
select 
    f.fee_id,
    f.fee_schedule_id,
    p.procedure_code,
    p.description as procedure_description,
    f.fee_amount,
    f.created_at,
    f.updated_at
from public_staging.stg_opendental__fee f
join public_staging.stg_opendental__procedurecode p on f.procedure_code_id = p.procedure_code_id
where f.fee_amount > 10000 or f.fee_amount < 0
order by f.fee_amount desc;

-- 2. High Fee Variation Records (78)
select 
    p.procedure_code,
    p.description as procedure_description,
    count(*) as fee_count,
    min(f.fee_amount) as min_fee,
    avg(f.fee_amount)::numeric(10,2) as avg_fee,
    max(f.fee_amount) as max_fee,
    (max(f.fee_amount) - min(f.fee_amount))::numeric(10,2) as fee_range
from public_staging.stg_opendental__fee f
join public_staging.stg_opendental__procedurecode p on f.procedure_code_id = p.procedure_code_id
where f.procedure_code_id in (
    select procedure_code_id
    from public_staging.stg_opendental__fee
    where fee_schedule_id not in (8285, 8290)
    group by procedure_code_id
    having max(fee_amount) - min(fee_amount) > 500
)
group by p.procedure_code, p.description
order by fee_range desc;

-- 3. Underutilized Fee Schedule Records (3)
select 
    f.fee_schedule_id,
    fs.fee_schedule_description,
    count(*) as fee_count,
    min(f.fee_amount) as min_fee,
    avg(f.fee_amount)::numeric(10,2) as avg_fee,
    max(f.fee_amount) as max_fee,
    string_agg(distinct p.procedure_code || ': ' || p.description, '; ') as procedures
from public_staging.stg_opendental__fee f
join public_staging.stg_opendental__feesched fs on f.fee_schedule_id = fs.fee_schedule_id
join public_staging.stg_opendental__procedurecode p on f.procedure_code_id = p.procedure_code_id
where f.fee_schedule_id in (
    select fee_schedule_id
    from public_staging.stg_opendental__fee
    group by fee_schedule_id
    having count(*) < 5
)
group by f.fee_schedule_id, fs.fee_schedule_description
order by fee_count;