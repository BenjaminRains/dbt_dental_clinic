{% test fee_default_zero(model) %}

with avg_fees as (
    select 
        procedure_code_id,
        avg(fee_amount) as avg_amount
    from {{ model }}
    group by procedure_code_id
)

select f.*
from {{ model }} as f
join avg_fees af on f.procedure_code_id = af.procedure_code_id
where f.is_default_fee = 1
    and f.fee_amount = 0
    and af.avg_amount > 0

{% endtest %} 