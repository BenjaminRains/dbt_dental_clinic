{% test fee_statistical_outlier(model) %}

with fee_stats as (
    select 
        procedure_code_id, 
        avg(fee_amount) as avg_amount, 
        stddev(fee_amount) as fee_stddev
    from {{ model }}
    group by procedure_code_id
)

select f.*
from {{ model }} as f
join fee_stats stats 
    on f.procedure_code_id = stats.procedure_code_id
where stats.fee_stddev > 0
    and abs(f.fee_amount - stats.avg_amount) > 2 * stats.fee_stddev

{% endtest %} 