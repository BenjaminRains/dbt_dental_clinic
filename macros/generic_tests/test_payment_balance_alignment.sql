{% test payment_balance_alignment(model, column_name=None) %}

with payment_totals as (
    select
        patient_id,
        sum(split_amount) as total_payment_amount
    from {{ model }}
    where include_in_ar = true
    group by patient_id
),

patient_balances as (
    select
        patient_id,
        total_balance
    from {{ ref('stg_opendental__patient') }}
),

validation as (
    select
        p.patient_id,
        p.total_balance,
        pt.total_payment_amount,
        abs(p.total_balance - pt.total_payment_amount) as balance_difference
    from patient_balances p
    left join payment_totals pt
        on p.patient_id = pt.patient_id
    where abs(p.total_balance - coalesce(pt.total_payment_amount, 0)) > 0.01
)

select *
from validation

{% endtest %} 