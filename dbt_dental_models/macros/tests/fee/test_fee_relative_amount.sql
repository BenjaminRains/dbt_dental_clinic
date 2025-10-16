{% test fee_relative_amount(model) %}

with fee_stats as (
    select 
        procedure_code_id,
        avg(fee_amount) as avg_amount,
        stddev(fee_amount) as std_amount
    from {{ model }}
    group by procedure_code_id
)

select f.*
from {{ model }} as f
where not (
    -- Known decimal errors
    f.fee_id in (217113, 219409, 218252)
    -- Inactive fee schedules
    or f.fee_schedule_id in (8285, 8290)
    -- Within acceptable statistical variation
    or exists (
        select 1
        from fee_stats s
        where s.procedure_code_id = f.procedure_code_id
        and abs(f.fee_amount - s.avg_amount) <= 3 * s.std_amount
    )
)

{% endtest %}