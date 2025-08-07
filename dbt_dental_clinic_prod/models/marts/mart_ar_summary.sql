{{ config(        materialized='table',
        
        unique_key=['date_id', 'patient_id', 'provider_id']) }}

/*
Accounts Receivable Summary Mart - AR aging and collection analysis
This mart provides comprehensive accounts receivable tracking including aging buckets,
collection rates, payment patterns, and outstanding balance management.

Key metrics:
- AR aging by patient and provider
- Collection effectiveness and timing
- Outstanding balance trends
- Payment plan management
- Write-off and adjustment analysis
*/

with PatientBalances as (
    select 
        dp.patient_id,
        dp.primary_provider_id,
        dp.clinic_id,
        dp.estimated_balance,
        dp.total_balance,
        dp.balance_0_30_days,
        dp.balance_31_60_days,
        dp.balance_61_90_days,
        dp.balance_over_90_days,
        dp.insurance_estimate,
        dp.payment_plan_due,
        dp.has_insurance_flag,
        dp.patient_status,
        dp.guarantor_id
    from {{ ref('dim_patient') }} dp
    where dp.patient_status in ('Patient', 'Inactive')
),

RecentClaims as (
    select 
        fc.patient_id,
        fc.provider_id,
        fc.claim_date,
        sum(fc.billed_amount) as total_billed,
        sum(fc.allowed_amount) as total_allowed,
        sum(fc.paid_amount) as total_paid,
        sum(fc.write_off_amount) as total_write_offs,
        sum(fc.patient_responsibility) as total_patient_responsibility,
        sum(fc.billed_amount - fc.paid_amount - fc.write_off_amount) as outstanding_claims,
        count(*) as claim_count,
        max(fc.claim_date) as last_claim_date,
        min(fc.claim_date) as first_claim_date
    from {{ ref('fact_claim') }} fc
    where fc.claim_date >= current_date - interval '365 days'
    group by fc.patient_id, fc.provider_id, fc.claim_date
),

RecentPayments as (
    select 
        fp.patient_id,
        fp.provider_id,
        fp.payment_date,
        sum(case when fp.is_patient_payment then fp.payment_amount else 0 end) as patient_payments,
        sum(case when fp.is_insurance_payment then fp.payment_amount else 0 end) as insurance_payments,
        sum(case when fp.is_adjustment then fp.payment_amount else 0 end) as adjustments,
        sum(fp.payment_amount) as total_payments,
        count(*) as payment_count,
        max(fp.payment_date) as last_payment_date,
        avg(fp.payment_amount) as avg_payment_amount
    from {{ ref('fact_payment') }} fp
    where fp.payment_date >= current_date - interval '365 days'
    group by fp.patient_id, fp.provider_id, fp.payment_date
),

/*
TODO - PAYMENT PLAN FUNCTIONALITY:
The PaymentPlans CTE has been temporarily disabled because stg_opendental__payplan
staging model has not been implemented yet. The clinic does not currently use 
payment plans, so this functionality is planned for future implementation.

When payment plan functionality is needed, implement stg_opendental__payplan:
1. Create stg_opendental__payplan staging model from payplan source table  
2. Include standard transformations: patient_id, payment_plan_amount, payment_plan_balance
3. Add business logic for is_closed, payment_plan_date, and status tracking
4. Consider payment plan templates and charge schedules

Expected structure when implemented:
- patient_id for linking to patient records
- active_payment_plans count for patients with open plans
- total_payment_plan_amount for original plan amounts
- total_payment_plan_balance for remaining balances
- earliest_plan_date and latest_plan_date for temporal analysis
*/

ARSnapshot as (
    select 
        current_date as snapshot_date,
        pb.patient_id,
        pb.primary_provider_id,
        pb.clinic_id,
        
        -- Current Balance Information
        pb.total_balance,
        pb.balance_0_30_days,
        pb.balance_31_60_days,
        pb.balance_61_90_days,
        pb.balance_over_90_days,
        pb.insurance_estimate,
        pb.payment_plan_due,
        
        -- Calculate patient responsibility
        greatest(pb.total_balance - coalesce(pb.insurance_estimate, 0), 0) as patient_responsibility,
        
        -- Recent Activity Aggregations
        sum(rc.total_billed) as billed_last_year,
        sum(rc.total_paid) as paid_last_year,
        sum(rc.total_write_offs) as write_offs_last_year,
        sum(rc.outstanding_claims) as outstanding_claims_amount,
        count(distinct rc.claim_date) as claim_days_last_year,
        max(rc.last_claim_date) as last_claim_date,
        
        -- Payment Activity
        sum(rp.patient_payments) as patient_payments_last_year,
        sum(rp.insurance_payments) as insurance_payments_last_year,
        sum(rp.adjustments) as adjustments_last_year,
        sum(rp.total_payments) as total_payments_last_year,
        count(distinct rp.payment_date) as payment_days_last_year,
        max(rp.last_payment_date) as last_payment_date,
        avg(rp.avg_payment_amount) as avg_payment_amount,
        
        -- Payment Plan Information (Not implemented - clinic doesn't use payment plans)
        0 as active_payment_plans,  -- TODO: Restore when stg_opendental__payplan is implemented
        0 as payment_plan_amount,   -- TODO: Restore when stg_opendental__payplan is implemented
        0 as payment_plan_balance,  -- TODO: Restore when stg_opendental__payplan is implemented
        
        -- Patient Characteristics
        pb.has_insurance_flag,
        pb.patient_status,
        pb.guarantor_id
        
    from PatientBalances pb
    left join RecentClaims rc
        on pb.patient_id = rc.patient_id
        and pb.primary_provider_id = rc.provider_id
    left join RecentPayments rp
        on pb.patient_id = rp.patient_id
        and pb.primary_provider_id = rp.provider_id
    -- TODO: Restore when stg_opendental__payplan is implemented:
    -- left join PaymentPlans ppl
    --     on pb.patient_id = ppl.patient_id
    group by 
        pb.patient_id, pb.primary_provider_id, pb.clinic_id,
        pb.total_balance, pb.balance_0_30_days, pb.balance_31_60_days,
        pb.balance_61_90_days, pb.balance_over_90_days, pb.insurance_estimate,
        pb.payment_plan_due, pb.has_insurance_flag, pb.patient_status, pb.guarantor_id
        -- TODO: Restore when stg_opendental__payplan is implemented:
        -- ppl.active_payment_plans, ppl.total_payment_plan_amount, ppl.total_payment_plan_balance
),

Final as (
    select
        -- Keys and Dimensions
        dd.date_id,
        ars.snapshot_date,
        ars.patient_id,
        ars.primary_provider_id as provider_id,
        ars.clinic_id,
        
        -- Provider Information
        prov.provider_name,
        prov.provider_type,
        prov.specialty,
        
        -- Patient Information
        pt.patient_status,
        pt.age,
        pt.gender,
        pt.has_insurance_flag,
        
        -- Date Information
        dd.year,
        dd.month,
        dd.quarter,
        dd.day_name,
        
        -- Current AR Balances
        ars.total_balance,
        ars.balance_0_30_days,
        ars.balance_31_60_days,
        ars.balance_61_90_days,
        ars.balance_over_90_days,
        ars.insurance_estimate,
        ars.payment_plan_due,
        ars.patient_responsibility,
        
        -- AR Aging Percentages
        round(ars.balance_0_30_days::numeric / nullif(ars.total_balance, 0) * 100, 2) as pct_current,
        round(ars.balance_31_60_days::numeric / nullif(ars.total_balance, 0) * 100, 2) as pct_31_60,
        round(ars.balance_61_90_days::numeric / nullif(ars.total_balance, 0) * 100, 2) as pct_61_90,
        round(ars.balance_over_90_days::numeric / nullif(ars.total_balance, 0) * 100, 2) as pct_over_90,
        
        -- Aging Risk Categories
        case 
            when ars.total_balance = 0 then 'No Balance'
            when ars.balance_over_90_days / nullif(ars.total_balance, 0) >= 0.5 then 'High Risk'
            when ars.balance_over_90_days / nullif(ars.total_balance, 0) >= 0.25 then 'Medium Risk'
            when ars.balance_61_90_days / nullif(ars.total_balance, 0) >= 0.5 then 'Moderate Risk'
            else 'Low Risk'
        end as aging_risk_category,
        
        -- Recent Activity Metrics
        ars.billed_last_year,
        ars.paid_last_year,
        ars.write_offs_last_year,
        ars.outstanding_claims_amount,
        ars.claim_days_last_year,
        ars.last_claim_date,
        
        -- Payment Activity Metrics
        ars.patient_payments_last_year,
        ars.insurance_payments_last_year,
        ars.adjustments_last_year,
        ars.total_payments_last_year,
        ars.payment_days_last_year,
        ars.last_payment_date,
        ars.avg_payment_amount,
        
        -- Collection Rates
        round(ars.total_payments_last_year::numeric / nullif(ars.billed_last_year, 0) * 100, 2) as collection_rate_last_year,
        round(ars.patient_payments_last_year::numeric / nullif(ars.patient_responsibility, 0) * 100, 2) as patient_collection_rate,
        round(ars.insurance_payments_last_year::numeric / nullif(ars.insurance_estimate, 0) * 100, 2) as insurance_collection_rate,
        
        -- Payment Plan Information
        ars.active_payment_plans,
        ars.payment_plan_amount,
        ars.payment_plan_balance,
        case when ars.active_payment_plans > 0 then true else false end as has_payment_plan,
        
        -- Activity Timing
        case 
            when ars.last_payment_date >= current_date - interval '30 days' then 'Recent'
            when ars.last_payment_date >= current_date - interval '90 days' then 'Moderate'
            when ars.last_payment_date >= current_date - interval '180 days' then 'Old'
            else 'Very Old'
        end as payment_recency,
        
        case 
            when ars.last_claim_date >= current_date - interval '30 days' then 'Recent'
            when ars.last_claim_date >= current_date - interval '90 days' then 'Moderate'
            when ars.last_claim_date >= current_date - interval '180 days' then 'Old'
            else 'Very Old'
        end as claim_recency,
        
        -- Collection Priority Scoring (0-100)
        round((
            case when ars.total_balance > 1000 then 25 else ars.total_balance / 40 end +
            case when ars.balance_over_90_days / nullif(ars.total_balance, 0) >= 0.5 then 25 else 0 end +
            case when ars.last_payment_date < current_date - interval '90 days' then 25 else 0 end +
            case when ars.has_insurance_flag = false then 25 else 0 end
        ), 0) as collection_priority_score,
        
        -- Balance Categories
        case 
            when ars.total_balance = 0 then 'No Balance'
            when ars.total_balance < 100 then 'Small'
            when ars.total_balance < 500 then 'Medium'
            when ars.total_balance < 2000 then 'Large'
            else 'Very Large'
        end as balance_category,
        
        -- Boolean Flags
        case when ars.total_balance > 0 then true else false end as has_outstanding_balance,
        case when ars.balance_over_90_days > 0 then true else false end as has_aged_balance,
        case when ars.insurance_estimate > 0 then true else false end as has_insurance_pending,
        case when ars.outstanding_claims_amount > 0 then true else false end as has_outstanding_claims,
        case when ars.last_payment_date >= current_date - interval '30 days' then true else false end as recent_payment_activity,
        
        -- Days Since Last Activity
        case when ars.last_payment_date is not null then current_date - ars.last_payment_date end as days_since_last_payment,
        case when ars.last_claim_date is not null then current_date - ars.last_claim_date end as days_since_last_claim,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from ARSnapshot ars
    inner join {{ ref('dim_date') }} dd
        on ars.snapshot_date = dd.date_actual
    inner join {{ ref('dim_provider') }} prov
        on ars.primary_provider_id = prov.provider_id
    inner join {{ ref('dim_patient') }} pt
        on ars.patient_id = pt.patient_id
    where ars.total_balance != 0 or ars.billed_last_year > 0
)

select * from Final
