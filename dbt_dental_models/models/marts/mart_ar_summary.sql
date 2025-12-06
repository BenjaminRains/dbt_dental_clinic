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
- ar_dimensions and payment_activity CTEs group only by patient_id to ensure one row per patient
  This prevents aggregation issues when summing billed_last_year across patients

Important Notes:
- This mart only includes patients with total_balance > 0 (outstanding AR)
- Patients with claims but no balance are excluded (appropriate for AR analysis)
- For practice-level AR Days calculations, query fact_claim directly (see AR service)
- The billed_last_year field represents patient-level billing for the last 365 days

Dependencies:
- dim_patient: Patient balance and demographic information
- fact_claim: Insurance billing and payment data
- fact_payment: Payment transaction details
- dim_provider: Provider information and relationships
- dim_date: Date dimension for temporal analysis
*/

-- 1. Base fact data with aging buckets calculated from int_ar_balance
-- Note: dim_patient has aging buckets hardcoded to 0.00, so we calculate from procedure-level data
-- IMPORTANT: Calculate at patient+provider level to avoid duplication when patient has multiple providers
with ar_aging_from_int as (
    select 
        ab.patient_id,
        ab.provider_id,
        -- Standard aging buckets (days since procedure)p-[===========================================================]
        sum(case when ab.aging_bucket = '0-30' then ab.current_balance else 0 end) as balance_0_30_days,
        sum(case when ab.aging_bucket = '31-60' then ab.current_balance else 0 end) as balance_31_60_days,
        sum(case when ab.aging_bucket = '61-90' then ab.current_balance else 0 end) as balance_61_90_days,
        sum(case when ab.aging_bucket = '90+' then ab.current_balance else 0 end) as balance_over_90_days,
        -- Practice by Numbers aging buckets (days overdue from due date)
        sum(case when ab.pbn_aging_bucket = 'CURRENT' then ab.current_balance else 0 end) as pbn_total_ar_current,
        sum(case when ab.pbn_aging_bucket = '30-60' then ab.current_balance else 0 end) as pbn_total_ar_30_60,
        sum(case when ab.pbn_aging_bucket = '60-90' then ab.current_balance else 0 end) as pbn_total_ar_60_90,
        sum(case when ab.pbn_aging_bucket = 'OVER_90' then ab.current_balance else 0 end) as pbn_total_ar_over_90,
        -- Calculate insurance estimate: insurance_estimate - insurance_payments already received
        sum(greatest(
            case 
                when coalesce(ab.insurance_estimate, 0) > 0 
                then coalesce(ab.insurance_estimate, 0) - coalesce(ab.insurance_payment_amount, 0)
                else 0
            end,
            0
        )) as insurance_estimate_from_procedures,
        sum(ab.current_balance) as total_balance_from_procedures
    from {{ ref('int_ar_balance') }} ab
    where ab.include_in_ar = true
        and ab.current_balance > 0
        -- Use 18 months to match int_ar_balance model's default, but allow older if needed
        and ab.procedure_date >= current_date - interval '18 months'
    group by ab.patient_id, ab.provider_id
),
-- Also need patient-level totals for patients without provider-specific data
ar_aging_patient_level as (
    select 
        ab.patient_id,
        -- Standard aging buckets (for fallback)
        sum(case when ab.aging_bucket = '0-30' then ab.current_balance else 0 end) as balance_0_30_days,
        sum(case when ab.aging_bucket = '31-60' then ab.current_balance else 0 end) as balance_31_60_days,
        sum(case when ab.aging_bucket = '61-90' then ab.current_balance else 0 end) as balance_61_90_days,
        sum(case when ab.aging_bucket = '90+' then ab.current_balance else 0 end) as balance_over_90_days,
        -- Practice by Numbers aging buckets (for fallback)
        sum(case when ab.pbn_aging_bucket = 'CURRENT' then ab.current_balance else 0 end) as pbn_total_ar_current,
        sum(case when ab.pbn_aging_bucket = '30-60' then ab.current_balance else 0 end) as pbn_total_ar_30_60,
        sum(case when ab.pbn_aging_bucket = '60-90' then ab.current_balance else 0 end) as pbn_total_ar_60_90,
        sum(case when ab.pbn_aging_bucket = 'OVER_90' then ab.current_balance else 0 end) as pbn_total_ar_over_90,
        -- Calculate insurance estimate
        sum(greatest(
            case 
                when coalesce(ab.insurance_estimate, 0) > 0 
                then coalesce(ab.insurance_estimate, 0) - coalesce(ab.insurance_payment_amount, 0)
                else 0
            end,
            0
        )) as insurance_estimate_from_procedures,
        sum(ab.current_balance) as total_balance_from_procedures
    from {{ ref('int_ar_balance') }} ab
    where ab.include_in_ar = true
        and ab.current_balance > 0
        and ab.procedure_date >= current_date - interval '18 months'
    group by ab.patient_id
),
ar_base as (
    select 
        dp.patient_id,
        dp.primary_provider_id,
        dp.clinic_id,
        dp.estimated_balance,
        dp.total_balance,
        -- Use calculated aging buckets from int_ar_balance at patient+provider level
        -- First try provider-specific, then fallback to patient-level, then 0
        coalesce(
            aai_provider.balance_0_30_days,
            aai_patient.balance_0_30_days,
            0.00
        ) as balance_0_30_days,
        coalesce(
            aai_provider.balance_31_60_days,
            aai_patient.balance_31_60_days,
            0.00
        ) as balance_31_60_days,
        coalesce(
            aai_provider.balance_61_90_days,
            aai_patient.balance_61_90_days,
            0.00
        ) as balance_61_90_days,
        coalesce(
            aai_provider.balance_over_90_days,
            aai_patient.balance_over_90_days,
            0.00
        ) as balance_over_90_days,
        -- Use calculated insurance estimate from procedures, prefer provider-specific
        coalesce(
            aai_provider.insurance_estimate_from_procedures,
            aai_patient.insurance_estimate_from_procedures,
            0.00
        ) as insurance_estimate,
        -- Practice by Numbers aging buckets (for scaling to dim_patient.total_balance)
        coalesce(
            aai_provider.pbn_total_ar_current,
            aai_patient.pbn_total_ar_current,
            0.00
        ) as pbn_total_ar_current_raw,
        coalesce(
            aai_provider.pbn_total_ar_30_60,
            aai_patient.pbn_total_ar_30_60,
            0.00
        ) as pbn_total_ar_30_60_raw,
        coalesce(
            aai_provider.pbn_total_ar_60_90,
            aai_patient.pbn_total_ar_60_90,
            0.00
        ) as pbn_total_ar_60_90_raw,
        coalesce(
            aai_provider.pbn_total_ar_over_90,
            aai_patient.pbn_total_ar_over_90,
            0.00
        ) as pbn_total_ar_over_90_raw,
        dp.payment_plan_due,
        dp.has_insurance_flag,
        dp.patient_status,
        dp.guarantor_id
    from {{ ref('dim_patient') }} dp
    left join ar_aging_from_int aai_provider
        on dp.patient_id = aai_provider.patient_id
        and dp.primary_provider_id = aai_provider.provider_id
    left join ar_aging_patient_level aai_patient
        on dp.patient_id = aai_patient.patient_id
    where dp.patient_status in ('Patient', 'Inactive')
        and dp.total_balance > 0  -- Only include patients with outstanding balance
),

-- 2. Dimension data
-- FIXED: Group only by patient_id (not claim_date) to ensure one row per patient
-- This prevents aggregation issues when summing billed_last_year across patients
ar_dimensions as (
    select 
        fc.patient_id,
        sum(fc.billed_amount) as total_billed,
        sum(fc.allowed_amount) as total_allowed,
        sum(fc.paid_amount) as total_paid,
        sum(fc.write_off_amount) as total_write_offs,
        sum(fc.patient_responsibility) as total_patient_responsibility,
        sum(fc.billed_amount - fc.paid_amount - fc.write_off_amount) as outstanding_claims,
        count(*) as claim_count,
        max(fc.claim_date) as last_claim_date,
        min(fc.claim_date) as first_claim_date,
        count(distinct fc.claim_date) as claim_days_last_year
    from {{ ref('fact_claim') }} fc
    where fc.claim_date >= current_date - interval '365 days'
    group by fc.patient_id
),

-- 3. Date dimension
date_dimension as (
    select * from {{ ref('dim_date') }}
),

-- 4. Payment activity data
-- FIXED: Group only by patient_id (not payment_date) to ensure one row per patient
-- This prevents aggregation issues when summing payment totals across patients
payment_activity as (
    select 
        fp.patient_id,
        sum(case when fp.is_patient_payment then fp.payment_amount else 0 end) as patient_payments,
        sum(case when fp.is_insurance_payment then fp.payment_amount else 0 end) as insurance_payments,
        sum(case when fp.is_adjustment then fp.payment_amount else 0 end) as adjustments,
        sum(fp.payment_amount) as total_payments,
        count(*) as payment_count,
        max(fp.payment_date) as last_payment_date,
        count(distinct fp.payment_date) as payment_days_last_year,
        avg(fp.payment_amount) as avg_payment_amount
    from {{ ref('fact_payment') }} fp
    where fp.payment_date >= current_date - interval '365 days'
    group by fp.patient_id
),

-- 5. Aggregations and calculations
ar_aggregated as (
    select 
        current_date as snapshot_date,
        pb.patient_id,
        pb.primary_provider_id,
        pb.clinic_id,
        
        -- Current Balance Information
        -- Use dim_patient.total_balance as source of truth, then scale buckets proportionally
        -- This ensures we match dim_patient totals while preserving aging proportions from int_ar_balance
        pb.total_balance as total_balance_from_dim_patient,
        -- Calculate sum of buckets from int_ar_balance
        (pb.balance_0_30_days + pb.balance_31_60_days + pb.balance_61_90_days + pb.balance_over_90_days) as total_balance_from_buckets,
        -- Scale buckets proportionally to match dim_patient.total_balance if different
        case 
            when (pb.balance_0_30_days + pb.balance_31_60_days + pb.balance_61_90_days + pb.balance_over_90_days) > 0
            then pb.total_balance * (pb.balance_0_30_days / nullif(pb.balance_0_30_days + pb.balance_31_60_days + pb.balance_61_90_days + pb.balance_over_90_days, 0))
            else 0.00
        end as balance_0_30_days,
        case 
            when (pb.balance_0_30_days + pb.balance_31_60_days + pb.balance_61_90_days + pb.balance_over_90_days) > 0
            then pb.total_balance * (pb.balance_31_60_days / nullif(pb.balance_0_30_days + pb.balance_31_60_days + pb.balance_61_90_days + pb.balance_over_90_days, 0))
            else 0.00
        end as balance_31_60_days,
        case 
            when (pb.balance_0_30_days + pb.balance_31_60_days + pb.balance_61_90_days + pb.balance_over_90_days) > 0
            then pb.total_balance * (pb.balance_61_90_days / nullif(pb.balance_0_30_days + pb.balance_31_60_days + pb.balance_61_90_days + pb.balance_over_90_days, 0))
            else 0.00
        end as balance_61_90_days,
        case 
            when (pb.balance_0_30_days + pb.balance_31_60_days + pb.balance_61_90_days + pb.balance_over_90_days) > 0
            then pb.total_balance * (pb.balance_over_90_days / nullif(pb.balance_0_30_days + pb.balance_31_60_days + pb.balance_61_90_days + pb.balance_over_90_days, 0))
            else 0.00
        end as balance_over_90_days,
        pb.total_balance as total_balance,  -- Use dim_patient total as source of truth
        pb.insurance_estimate,
        pb.payment_plan_due,
        
        -- Calculate patient responsibility based on dim_patient.total_balance
        greatest(pb.total_balance - coalesce(pb.insurance_estimate, 0), 0) as patient_responsibility,
        
        -- Practice by Numbers metrics (scaled proportionally to match dim_patient.total_balance)
        -- Scale PBN buckets proportionally to match total_balance
        case 
            when (pb.pbn_total_ar_current_raw + pb.pbn_total_ar_30_60_raw + pb.pbn_total_ar_60_90_raw + pb.pbn_total_ar_over_90_raw) > 0
            then pb.total_balance * (pb.pbn_total_ar_current_raw / nullif(pb.pbn_total_ar_current_raw + pb.pbn_total_ar_30_60_raw + pb.pbn_total_ar_60_90_raw + pb.pbn_total_ar_over_90_raw, 0))
            else 0.00
        end as pbn_total_ar_current,
        case 
            when (pb.pbn_total_ar_current_raw + pb.pbn_total_ar_30_60_raw + pb.pbn_total_ar_60_90_raw + pb.pbn_total_ar_over_90_raw) > 0
            then pb.total_balance * (pb.pbn_total_ar_30_60_raw / nullif(pb.pbn_total_ar_current_raw + pb.pbn_total_ar_30_60_raw + pb.pbn_total_ar_60_90_raw + pb.pbn_total_ar_over_90_raw, 0))
            else 0.00
        end as pbn_total_ar_30_60,
        case 
            when (pb.pbn_total_ar_current_raw + pb.pbn_total_ar_30_60_raw + pb.pbn_total_ar_60_90_raw + pb.pbn_total_ar_over_90_raw) > 0
            then pb.total_balance * (pb.pbn_total_ar_60_90_raw / nullif(pb.pbn_total_ar_current_raw + pb.pbn_total_ar_30_60_raw + pb.pbn_total_ar_60_90_raw + pb.pbn_total_ar_over_90_raw, 0))
            else 0.00
        end as pbn_total_ar_60_90,
        case 
            when (pb.pbn_total_ar_current_raw + pb.pbn_total_ar_30_60_raw + pb.pbn_total_ar_60_90_raw + pb.pbn_total_ar_over_90_raw) > 0
            then pb.total_balance * (pb.pbn_total_ar_over_90_raw / nullif(pb.pbn_total_ar_current_raw + pb.pbn_total_ar_30_60_raw + pb.pbn_total_ar_60_90_raw + pb.pbn_total_ar_over_90_raw, 0))
            else 0.00
        end as pbn_total_ar_over_90,
        
        -- Recent Activity Aggregations
        -- FIXED: Use direct values from ar_dimensions (already aggregated by patient_id)
        -- Since ar_dimensions now has one row per patient, SUM works correctly across all patients
        coalesce(sum(rc.total_billed), 0) as billed_last_year,
        coalesce(sum(rc.total_paid), 0) as paid_last_year,
        coalesce(sum(rc.total_write_offs), 0) as write_offs_last_year,
        coalesce(sum(rc.outstanding_claims), 0) as outstanding_claims_amount,
        coalesce(sum(rc.claim_days_last_year), 0) as claim_days_last_year,
        max(rc.last_claim_date) as last_claim_date,
        
        -- Payment Activity
        -- FIXED: Use direct values from payment_activity (already aggregated by patient_id)
        -- Since payment_activity now has one row per patient, SUM works correctly across all patients
        coalesce(sum(pa.patient_payments), 0) as patient_payments_last_year,
        coalesce(sum(pa.insurance_payments), 0) as insurance_payments_last_year,
        coalesce(sum(pa.adjustments), 0) as adjustments_last_year,
        coalesce(sum(pa.total_payments), 0) as total_payments_last_year,
        coalesce(sum(pa.payment_days_last_year), 0) as payment_days_last_year,
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
        pb.pbn_total_ar_current_raw, pb.pbn_total_ar_30_60_raw, pb.pbn_total_ar_60_90_raw, pb.pbn_total_ar_over_90_raw,
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
        
        -- Practice by Numbers Percentages
        case 
            when total_balance = 0 then 0.00
            else round((pbn_total_ar_current::numeric / nullif(total_balance, 0) * 100)::numeric, 2)
        end as pbn_total_ar_current_pct,
        case 
            when total_balance = 0 then 0.00
            else round((pbn_total_ar_30_60::numeric / nullif(total_balance, 0) * 100)::numeric, 2)
        end as pbn_total_ar_30_60_pct,
        case 
            when total_balance = 0 then 0.00
            else round((pbn_total_ar_60_90::numeric / nullif(total_balance, 0) * 100)::numeric, 2)
        end as pbn_total_ar_60_90_pct,
        case 
            when total_balance = 0 then 0.00
            else round((pbn_total_ar_over_90::numeric / nullif(total_balance, 0) * 100)::numeric, 2)
        end as pbn_total_ar_over_90_pct,
        
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
        greatest(0, least(100,
            round((
                case when total_balance > 1000 then 25 else total_balance / 40 end +
                case when balance_over_90_days / nullif(total_balance, 0) >= 0.5 then 25 else 0 end +
                case when last_payment_date < current_date - interval '90 days' then 25 else 0 end +
                case when has_insurance_flag = false then 25 else 0 end
            )::numeric, 0)
        )) as collection_priority_score,
        
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
        
        -- Practice by Numbers aging buckets
        ae.pbn_total_ar_current,
        ae.pbn_total_ar_30_60,
        ae.pbn_total_ar_60_90,
        ae.pbn_total_ar_over_90,
        
        -- Practice by Numbers percentages
        ae.pbn_total_ar_current_pct,
        ae.pbn_total_ar_30_60_pct,
        ae.pbn_total_ar_60_90_pct,
        ae.pbn_total_ar_over_90_pct,
        
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