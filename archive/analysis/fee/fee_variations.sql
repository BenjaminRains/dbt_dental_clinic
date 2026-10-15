/*
Fee Analysis Queries
-------------------
These queries analyze fee variations and potential data quality issues in dental procedure fees.
They focus on identifying:
1. Procedures with large fee variations (>$500 range)
2. Fee schedules with unusual patterns
3. High-cost procedures and their actual usage

Key findings:
- MDC fee schedules show extreme variations ($0-$9999)
- Some procedures have $25,500 fees but don't exist in procedure code table
- Many null procedure codes with set fees
- Identical fee patterns between MDC EDP and SPOUSE/CHILDREN schedules
*/

-- Query 1: Analyze procedures with high fee variations
with fee_stats as (
    select 
        f.procedure_code_id,
        count(*) as fee_count,
        min(f.fee_amount) as min_fee,
        max(f.fee_amount) as max_fee,
        avg(f.fee_amount) as avg_fee,
        max(f.fee_amount) - min(f.fee_amount) as fee_range,
        stddev(f.fee_amount) as fee_stddev
    from staging.stg_opendental__fee f
    group by f.procedure_code_id
    having max(f.fee_amount) - min(f.fee_amount) > 500
)
select 
    fs.*,
    p.procedure_code,
    p.description as procedure_description,
    p.abbreviated_description,
    p.procedure_category_id,
    count(distinct f.fee_schedule_id) as num_fee_schedules,
    string_agg(distinct fs2.fee_schedule_description, ', ') as fee_schedule_names,
    count(distinct pl.procedure_id) as times_performed,
    avg(pl.procedure_fee) as avg_actual_fee_charged
from fee_stats fs
left join staging.stg_opendental__procedurecode p 
    on fs.procedure_code_id = p.procedure_code_id
left join staging.stg_opendental__fee f 
    on fs.procedure_code_id = f.procedure_code_id
left join staging.stg_opendental__feesched fs2 
    on f.fee_schedule_id = fs2.fee_schedule_id
left join staging.stg_opendental__procedurelog pl 
    on fs.procedure_code_id = pl.code_id
group by 
    fs.procedure_code_id,
    fs.fee_count,
    fs.min_fee,
    fs.max_fee,
    fs.avg_fee,
    fs.fee_range,
    fs.fee_stddev,
    p.procedure_code,
    p.description,
    p.abbreviated_description,
    p.procedure_category_id
order by fs.fee_range desc;

-- Query 2: Analyze fee schedules containing high variations
with fee_stats as (
    select 
        f.procedure_code_id,
        count(*) as fee_count,
        min(f.fee_amount) as min_fee,
        max(f.fee_amount) as max_fee,
        max(f.fee_amount) - min(f.fee_amount) as fee_range
    from staging.stg_opendental__fee f
    group by f.procedure_code_id
    having max(f.fee_amount) - min(f.fee_amount) > 500
)
select 
    f.fee_schedule_id,
    fs.fee_schedule_description,
    count(distinct f.procedure_code_id) as num_procedures,
    avg(f.fee_amount) as avg_fee,
    stddev(f.fee_amount) as fee_stddev,
    min(f.fee_amount) as min_fee,
    max(f.fee_amount) as max_fee,
    count(distinct case when f.fee_amount > 1000 then f.procedure_code_id end) as high_cost_procedures,
    string_agg(distinct p.procedure_code || ': ' || p.description, '; ') as high_variation_procedures
from staging.stg_opendental__fee f
join staging.stg_opendental__feesched fs 
    on f.fee_schedule_id = fs.fee_schedule_id
left join staging.stg_opendental__procedurecode p
    on f.procedure_code_id = p.procedure_code_id
where f.procedure_code_id in (select procedure_code_id from fee_stats)
group by f.fee_schedule_id, fs.fee_schedule_description
order by fee_stddev desc;

-- Query 3: Detailed analysis of high-cost procedures in MDC fee schedules
select 
    f.fee_schedule_id,
    fs.fee_schedule_description,
    f.procedure_code_id,
    p.procedure_code,
    p.description as procedure_description,
    f.fee_amount,
    count(pl.procedure_id) as times_used,
    avg(pl.procedure_fee) as actual_fee_charged
from staging.stg_opendental__fee f
join staging.stg_opendental__feesched fs 
    on f.fee_schedule_id = fs.fee_schedule_id
left join staging.stg_opendental__procedurecode p
    on f.procedure_code_id = p.procedure_code_id
left join staging.stg_opendental__procedurelog pl
    on f.procedure_code_id = pl.code_id
where f.fee_schedule_id in (8292, 8293)  -- MDC fee schedules
    and f.fee_amount > 1000  -- High-cost procedures only
group by 
    f.fee_schedule_id,
    fs.fee_schedule_description,
    f.procedure_code_id,
    p.procedure_code,
    p.description,
    f.fee_amount
order by f.fee_amount desc;