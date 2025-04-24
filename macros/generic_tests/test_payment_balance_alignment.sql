{% test payment_balance_alignment(model, column_name=None) %}

with payment_totals as (
    select
        patient_id,
        sum(case when payment_type_id = 0 and payment_notes like '%INCOME TRANSFER%' then 0 else split_amount end) as total_payment_amount,
        count(distinct case when payment_type_id = 0 and payment_notes like '%INCOME TRANSFER%' then null else payment_id end) as payment_count,
        sum(case when payment_type_id = 72 then split_amount else 0 end) as total_refunds,
        count(distinct case when payment_type_id = 71 then payment_id end) as type_71_count,
        count(distinct case when payment_type_id = 412 then payment_id end) as type_412_count,
        sum(case when payment_type_id = 71 then split_amount else 0 end) as type_71_amount,
        sum(case when payment_type_id = 412 then split_amount else 0 end) as type_412_amount
    from {{ ref('int_patient_payment_allocated') }}
    group by patient_id
),

patient_balances as (
    select
        patient_id,
        balance_0_30_days as Bal_0_30,
        balance_31_60_days as Bal_31_60,
        balance_61_90_days as Bal_61_90,
        balance_over_90_days as BalOver90,
        (balance_0_30_days + balance_31_60_days + balance_61_90_days + balance_over_90_days) as aged_balance,
        CASE 
            WHEN balance_over_90_days > 0 THEN '90+ included'
            WHEN balance_61_90_days > 0 THEN '61-90 only'
            WHEN balance_31_60_days > 0 THEN '31-60 only'
            WHEN balance_0_30_days > 0 THEN '0-30 only'
            ELSE 'No balance'
        END as balance_aging_pattern
    from {{ ref('stg_opendental__patient') }}
),

validation as (
    select
        p.patient_id,
        p.aged_balance,
        pt.total_payment_amount,
        abs(p.aged_balance - pt.total_payment_amount) as difference,
        CASE 
            WHEN p.aged_balance > pt.total_payment_amount THEN 'Patient balance higher'
            ELSE 'Payment total higher'
        END as discrepancy_type,
        pt.payment_count,
        pt.total_refunds,
        pt.type_71_count,
        pt.type_412_count,
        pt.type_71_amount,
        pt.type_412_amount,
        p.balance_aging_pattern
    from patient_balances p
    join payment_totals pt on p.patient_id = pt.patient_id
    where abs(p.aged_balance - pt.total_payment_amount) > 0.01
    and not (
        -- Exclude cases with high value payments
        pt.total_payment_amount > 5000 or
        -- Exclude cases with many payments
        pt.payment_count > 20 or
        -- Exclude cases with large refunds
        abs(pt.total_refunds) > 1000 or
        -- Exclude very recent payments (within last 7 days)
        exists (
            select 1 
            from {{ ref('int_patient_payment_allocated') }} ppa
            where ppa.patient_id = p.patient_id
            and ppa.payment_date >= current_date - interval '7 days'
            and ppa.payment_type_id != 0
        ) or
        -- Exclude cases where all payments are recent
        not exists (
            select 1 
            from {{ ref('int_patient_payment_allocated') }} ppa
            where ppa.patient_id = p.patient_id
            and ppa.payment_date < current_date - interval '7 days'
            and ppa.payment_type_id != 0
        ) or
        -- Exclude cases with simple aging patterns
        p.balance_aging_pattern in ('0-30 only', '31-60 only')
    )
)

select *
from validation

{% endtest %} 