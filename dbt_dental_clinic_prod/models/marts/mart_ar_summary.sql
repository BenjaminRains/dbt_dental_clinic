{{ config(
        materialized='table',
        schema='marts',
        unique_key=['date_id', 'patient_id', 'provider_id'],
        on_schema_change='fail',
        indexes=[
            {'columns': ['date_id']},
            {'columns': ['patient_id']},
            {'columns': ['provider_id']},
            {'columns': ['clinic_id']}
        ]
    ) }}

/*
Summary Mart model for Accounts Receivable Analysis
Part of System A: Financial Management

This model:
1. Provides comprehensive accounts receivable tracking and aging analysis
2. Calculates collection rates and payment performance metrics
3. Supports AR management and collection prioritization workflows

Business Logic Features:
- AR aging buckets (0-30, 31-60, 61-90, 90+ days)
- Collection rate calculations for patient and insurance payments
- Risk categorization based on aging patterns
- Payment plan tracking (currently unused by clinic)
- Collection priority scoring for workflow optimization

Key Metrics:
- AR aging percentages by time bucket
- Collection rates (overall, patient, insurance)
- Outstanding balance trends and risk categories
- Payment timing and recency analysis
- Collection priority scores (0-100 scale)

Data Quality Notes:
- Payment plan functionality not used by clinic (fields set to defaults)
- Balances may include estimates and pending insurance amounts
- Historical data limited to 365 days for recent activity calculations

Performance Considerations:
- Indexed on date_id, patient_id, provider_id, clinic_id for query performance
- Aggregations performed at patient-provider level to optimize data volume
- Date dimension join required for temporal analysis

Dependencies:
- dim_patient: Patient balance and demographic information
- fact_claim: Insurance billing and payment data
- fact_payment: Payment transaction details
- dim_provider: Provider information and relationships
- dim_date: Date dimension for temporal analysis
*/

-- 1. Base fact data
with ar_base as (
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

-- 2. Dimension data
ar_dimensions as (
    select 
        fc.patient_id,
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
    group by fc.patient_id, fc.claim_date
),

-- 3. Date dimension
date_dimension as (
    select * from {{ ref('dim_date') }}
),

-- 4. Payment activity data
payment_activity as (
    select 
        fp.patient_id,
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
    group by fp.patient_id, fp.payment_date
),

-- 5. Aggregations and calculations
ar_aggregated as (
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
        sum(pa.patient_payments) as patient_payments_last_year,
        sum(pa.insurance_payments) as insurance_payments_last_year,
        sum(pa.adjustments) as adjustments_last_year,
        sum(pa.total_payments) as total_payments_last_year,
        count(distinct pa.payment_date) as payment_days_last_year,
        max(pa.last_payment_date) as last_payment_date,
        avg(pa.avg_payment_amount) as avg_payment_amount,
        
        -- Payment Plan Information (Not used by clinic)
        0 as active_payment_plans,
        0 as payment_plan_amount,
        0 as payment_plan_balance,
        
        -- Patient Characteristics
        pb.has_insurance_flag,
        pb.patient_status,
        pb.guarantor_id
        
    from ar_base pb
    left join ar_dimensions rc
        on pb.patient_id = rc.patient_id
    left join payment_activity pa
        on pb.patient_id = pa.patient_id
    -- Payment plan functionality not used by clinic
    group by 
        pb.patient_id, pb.primary_provider_id, pb.clinic_id,
        pb.total_balance, pb.balance_0_30_days, pb.balance_31_60_days,
        pb.balance_61_90_days, pb.balance_over_90_days, pb.insurance_estimate,
        pb.payment_plan_due, pb.has_insurance_flag, pb.patient_status, pb.guarantor_id
        -- Payment plan functionality not used by clinic
),

-- 6. Business logic enhancement
ar_enhanced as (
    select
        *,
        -- AR Aging Percentages
        case 
            when total_balance = 0 then 0.00
            else round((balance_0_30_days::numeric / nullif(total_balance, 0) * 100)::numeric, 2)
        end as pct_current,
        case 
            when total_balance = 0 then 0.00
            else round((balance_31_60_days::numeric / nullif(total_balance, 0) * 100)::numeric, 2)
        end as pct_31_60,
        case 
            when total_balance = 0 then 0.00
            else round((balance_61_90_days::numeric / nullif(total_balance, 0) * 100)::numeric, 2)
        end as pct_61_90,
        case 
            when total_balance = 0 then 0.00
            else round((balance_over_90_days::numeric / nullif(total_balance, 0) * 100)::numeric, 2)
        end as pct_over_90,
        
        -- Aging Risk Categories
        case 
            when total_balance = 0 then 'No Balance'
            when total_balance < 0 then 'Credit Balance'
            when balance_over_90_days / nullif(total_balance, 0) >= 0.5 then 'High Risk'
            when balance_over_90_days / nullif(total_balance, 0) >= 0.25 then 'Medium Risk'
            when balance_61_90_days / nullif(total_balance, 0) >= 0.5 then 'Moderate Risk'
            else 'Low Risk'
        end as aging_risk_category,
        
        -- Collection Rates
        case 
            when billed_last_year = 0 then 0.00
            else round((total_payments_last_year::numeric / nullif(billed_last_year, 0) * 100)::numeric, 2)
        end as collection_rate_last_year,
        case 
            when patient_responsibility = 0 then 0.00
            else round((patient_payments_last_year::numeric / nullif(patient_responsibility, 0) * 100)::numeric, 2)
        end as patient_collection_rate,
        case 
            when insurance_estimate = 0 then 0.00
            else round((insurance_payments_last_year::numeric / nullif(insurance_estimate, 0) * 100)::numeric, 2)
        end as insurance_collection_rate,
        
        -- Payment Plan Information
        case when active_payment_plans > 0 then true else false end as has_payment_plan,
        
        -- Activity Timing
        case 
            when last_payment_date >= current_date - interval '30 days' then 'Recent'
            when last_payment_date >= current_date - interval '90 days' then 'Moderate'
            when last_payment_date >= current_date - interval '180 days' then 'Old'
            else 'Very Old'
        end as payment_recency,
        
        case 
            when last_claim_date >= current_date - interval '30 days' then 'Recent'
            when last_claim_date >= current_date - interval '90 days' then 'Moderate'
            when last_claim_date >= current_date - interval '180 days' then 'Old'
            else 'Very Old'
        end as claim_recency,
        
        -- Collection Priority Scoring (0-100)
        round((
            case when total_balance > 1000 then 25 else total_balance / 40 end +
            case when balance_over_90_days / nullif(total_balance, 0) >= 0.5 then 25 else 0 end +
            case when last_payment_date < current_date - interval '90 days' then 25 else 0 end +
            case when has_insurance_flag = false then 25 else 0 end
        )::numeric, 0) as collection_priority_score,
        
        -- Balance Categories
        case 
            when total_balance = 0 then 'No Balance'
            when total_balance < 0 then 'Credit Balance'
            when total_balance < 100 then 'Small'
            when total_balance < 500 then 'Medium'
            when total_balance < 2000 then 'Large'
            else 'Very Large'
        end as balance_category,
        
        -- Boolean Flags
        case when total_balance > 0 then true else false end as has_outstanding_balance,
        case when balance_over_90_days > 0 then true else false end as has_aged_balance,
        case when insurance_estimate > 0 then true else false end as has_insurance_pending,
        case when outstanding_claims_amount > 0 then true else false end as has_outstanding_claims,
        case when last_payment_date >= current_date - interval '30 days' then true else false end as recent_payment_activity,
        
        -- Days Since Last Activity
        case when last_payment_date is not null then current_date - last_payment_date end as days_since_last_payment,
        case when last_claim_date is not null then current_date - last_claim_date end as days_since_last_claim
        
    from ar_aggregated
),

-- 7. Final integration
final as (
    select
        -- Date and dimensions
        dd.date_id,
        ae.snapshot_date,
        ae.patient_id,
        ae.primary_provider_id as provider_id,
        ae.clinic_id,
        
        -- Provider Information
        prov.provider_first_name,
        prov.provider_last_name,
        prov.provider_type_category,
        prov.specialty_description,
        
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
        ae.total_balance,
        ae.balance_0_30_days,
        ae.balance_31_60_days,
        ae.balance_61_90_days,
        ae.balance_over_90_days,
        ae.insurance_estimate,
        ae.payment_plan_due,
        ae.patient_responsibility,
        
        -- Calculated metrics from enhanced CTE
        ae.pct_current,
        ae.pct_31_60,
        ae.pct_61_90,
        ae.pct_over_90,
        ae.aging_risk_category,
        
        -- Recent Activity Metrics
        ae.billed_last_year,
        ae.paid_last_year,
        ae.write_offs_last_year,
        ae.outstanding_claims_amount,
        ae.claim_days_last_year,
        ae.last_claim_date,
        
        -- Payment Activity Metrics
        ae.patient_payments_last_year,
        ae.insurance_payments_last_year,
        ae.adjustments_last_year,
        ae.total_payments_last_year,
        ae.payment_days_last_year,
        ae.last_payment_date,
        ae.avg_payment_amount,
        
        -- Collection Rates
        ae.collection_rate_last_year,
        ae.patient_collection_rate,
        ae.insurance_collection_rate,
        
        -- Payment Plan Information
        ae.active_payment_plans,
        ae.payment_plan_amount,
        ae.payment_plan_balance,
        ae.has_payment_plan,
        
        -- Activity Timing
        ae.payment_recency,
        ae.claim_recency,
        
        -- Collection Priority Scoring
        ae.collection_priority_score,
        
        -- Balance Categories
        ae.balance_category,
        
        -- Boolean Flags
        ae.has_outstanding_balance,
        ae.has_aged_balance,
        ae.has_insurance_pending,
        ae.has_outstanding_claims,
        ae.recent_payment_activity,
        
        -- Days Since Last Activity
        ae.days_since_last_payment,
        ae.days_since_last_claim,
        
        -- Metadata
        {{ standardize_mart_metadata() }}
        
    from ar_enhanced ae
    inner join date_dimension dd
        on ae.snapshot_date = dd.date_day
    inner join {{ ref('dim_provider') }} prov
        on ae.primary_provider_id = prov.provider_id
    inner join {{ ref('dim_patient') }} pt
        on ae.patient_id = pt.patient_id
    where ae.total_balance != 0 or ae.billed_last_year > 0
)

select * from Final
