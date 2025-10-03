{% test fee_variation(model) %}

with proc_code_ranges as (
    select 
        procedure_code_id,
        max(fee_amount) - min(fee_amount) as fee_range
    from {{ model }}
    group by procedure_code_id
)

select f.*
from {{ model }} as f
where not (
    -- Known high variation procedure codes
    f.ada_code in (
        'D6040', 'D5725', 'D6055', 'D6010',
        'D5863', 'D5865',
        'D6059', 'D6065', 'D6061', 'D6066',
        'D2740'
    )
    -- Known decimal errors
    or f.fee_id in (217113, 219409, 218252)
    -- Inactive fee schedules
    or f.fee_schedule_id in (8285, 8290)
    -- Procedure has low fee variation
    or exists (
        select 1
        from proc_code_ranges pcr
        where pcr.procedure_code_id = f.procedure_code_id
        and pcr.fee_range <= 500
    )
)

{% endtest %}