{% test fee_schedule_usage(model) %}

with schedule_counts as (
    select 
        fee_schedule_id,
        count(*) as fee_count
    from {{ model }}
    group by fee_schedule_id
)

select f.*
from {{ model }} as f
join schedule_counts sc on f.fee_schedule_id = sc.fee_schedule_id
where f.fee_schedule_id not in (8285, 8290)  -- Exclude known inactive schedules
    and sc.fee_count < 5

{% endtest %} 