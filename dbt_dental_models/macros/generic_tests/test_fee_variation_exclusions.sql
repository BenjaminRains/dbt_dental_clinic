{% test fee_variation_exclusions(model) %}

with known_high_variations as (
    select *
    from {{ model }}
    where ada_code in (
        'D6040', 'D5725', 'D6055', 'D6010',
        'D5863', 'D5865',
        'D6059', 'D6065', 'D6061', 'D6066',
        'D2740'
    )
),

known_errors as (
    select *
    from {{ model }}
    where fee_id in (217113, 219409, 218252)
),

inactive_schedules as (
    select *
    from {{ model }}
    where fee_schedule_id in (8285, 8290)
),

fee_ranges as (
    select
        procedure_code_id,
        max(fee_amount) - min(fee_amount) as fee_range
    from {{ model }}
    group by procedure_code_id
),

suspicious_variations as (
    select f.*
    from {{ model }} f
    join fee_ranges fr on f.procedure_code_id = fr.procedure_code_id
    where fr.fee_range > 2000
    and f.ada_code not in (
        'D6040', 'D5725', 'D6055', 'D6010',
        'D5863', 'D5865',
        'D6059', 'D6065', 'D6061', 'D6066',
        'D2740'
    )
    and f.fee_id not in (217113, 219409, 218252)
    and f.fee_schedule_id not in (8285, 8290)
)

select * from suspicious_variations

{% endtest %}