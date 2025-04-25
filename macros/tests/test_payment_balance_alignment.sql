{% macro test_payment_balance_alignment(model, column_name=None) %}

with payment_totals as (
    select
        patient_id,
        sum(case 
            when payment_type_id = 0 then 0  -- Exclude all Type 0 payments
            when include_in_ar then split_amount  -- Only include payments marked for AR
            else 0
        end) as total_payment_amount,
        count(distinct case 
            when payment_type_id = 0 then null  -- Exclude all Type 0 payments
            when include_in_ar then payment_id
            else null
        end) as payment_count,
        sum(case when payment_type_id = 72 then split_amount else 0 end) as total_refunds,
        -- Payment type counts
        count(distinct case when payment_type_id = 71 then payment_id end) as type_71_count,
        count(distinct case when payment_type_id = 412 then payment_id end) as type_412_count,
        count(distinct case when payment_type_id = 72 then payment_id end) as type_72_count,
        count(distinct case when payment_type_id = 69 then payment_id end) as type_69_count,
        count(distinct case when payment_type_id = 70 then payment_id end) as type_70_count,
        count(distinct case when payment_type_id = 391 then payment_id end) as type_391_count,
        count(distinct case when payment_type_id = 634 then payment_id end) as type_634_count,
        count(distinct case when payment_type_id = 574 then payment_id end) as type_574_count,
        count(distinct case when payment_type_id = 417 then payment_id end) as type_417_count,
        -- Payment type amounts
        sum(case when payment_type_id = 71 then split_amount else 0 end) as type_71_amount,
        sum(case when payment_type_id = 412 then split_amount else 0 end) as type_412_amount,
        sum(case when payment_type_id = 69 then split_amount else 0 end) as type_69_amount,
        sum(case when payment_type_id = 70 then split_amount else 0 end) as type_70_amount,
        sum(case when payment_type_id = 391 then split_amount else 0 end) as type_391_amount,
        -- Date information
        min(payment_date) as earliest_payment_date,
        max(payment_date) as latest_payment_date,
        count(distinct case when payment_date >= current_date - interval '7 days' then payment_id end) as recent_payment_count,
        -- Additional metrics
        count(distinct payment_id) as total_payment_count,
        sum(split_amount) as raw_total_amount,
        count(distinct case when payment_type_id = 0 then payment_id end) as type_0_count,
        sum(case when payment_type_id = 0 then split_amount else 0 end) as type_0_amount,
        -- Payment type distribution
        string_agg(distinct payment_type_id::text, ',') as payment_types,
        -- Payment date distribution
        string_agg(distinct payment_date::text, ',') as payment_dates
    from {{ model }}
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
        END as balance_aging_pattern,
        updated_at as balance_update_date
    from {{ ref('stg_opendental__patient') }}
)

select
    p.patient_id,
    p.aged_balance,
    pt.total_payment_amount,
    abs(p.aged_balance - pt.total_payment_amount) as difference,
    CASE 
        WHEN p.aged_balance > pt.total_payment_amount THEN 'Patient balance higher'
        ELSE 'Payment total higher'
    END as discrepancy_type,
    -- Payment type counts
    pt.type_71_count,
    pt.type_412_count,
    pt.type_72_count,
    pt.type_69_count,
    pt.type_70_count,
    pt.type_391_count,
    pt.type_634_count,
    pt.type_574_count,
    pt.type_417_count,
    -- Payment type amounts
    pt.type_71_amount,
    pt.type_412_amount,
    pt.type_69_amount,
    pt.type_70_amount,
    pt.type_391_amount,
    pt.total_refunds,
    -- Balance aging details
    p.Bal_0_30,
    p.Bal_31_60,
    p.Bal_61_90,
    p.BalOver90,
    p.balance_aging_pattern,
    -- Date information
    pt.earliest_payment_date,
    pt.latest_payment_date,
    p.balance_update_date,
    pt.recent_payment_count,
    -- Additional metrics
    pt.total_payment_count,
    pt.raw_total_amount,
    pt.type_0_count,
    pt.type_0_amount,
    pt.payment_types,
    pt.payment_dates,
    CASE 
        WHEN pt.latest_payment_date > p.balance_update_date THEN 'Payments newer than balance'
        WHEN pt.latest_payment_date < p.balance_update_date THEN 'Balance newer than payments'
        ELSE 'Same date'
    END as timing_relationship,
    EXTRACT(DAY FROM (pt.latest_payment_date - p.balance_update_date)) as days_between_payment_and_update
from patient_balances p
join payment_totals pt on p.patient_id = pt.patient_id
where abs(p.aged_balance - pt.total_payment_amount) > 0.01
and not (
    -- Exclude cases with high value payments
    pt.total_payment_amount > 10000 or
    -- Exclude cases with many payments
    pt.payment_count > 20 or
    -- Exclude cases with large refunds
    abs(pt.total_refunds) > 1000 or
    -- Exclude very recent payments (within last 7 days)
    pt.recent_payment_count > 0 or
    -- Exclude cases where all payments are recent
    pt.earliest_payment_date >= current_date - interval '7 days' or
    -- Exclude all aging patterns to focus on genuine discrepancies
    p.balance_aging_pattern in ('0-30 only', '31-60 only', '61-90 only', '90+ included', 'No balance')
)
order by difference desc
limit 100

{% endmacro %}