{{
    config(
        materialized='table',
        schema='marts',
        unique_key='insurance_plan_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['insurance_plan_id'], 'unique': true},
            {'columns': ['_updated_at']},
            {'columns': ['patient_id']},
            {'columns': ['carrier_id']},
            {'columns': ['is_active']}
        ]
    )
}}

/*
Dimension model for insurance plans and carriers.
Part of System G: Insurance and Claims Management

This model:
1. Provides comprehensive insurance plan and carrier information for all downstream analytics
2. Tracks historical changes and current status of insurance plans
3. Calculates performance metrics for claims processing and reimbursement

Business Logic Features:
- Network status tracking based on verification dates and active status
- Claim performance metrics including approval rates and reimbursement rates
- Employer information integration for group plans
- Historical tracking capabilities for plan changes

Key Metrics:
- claim_approval_rate: Percentage of claims approved by this insurance plan
- average_reimbursement_rate: Average percentage of billed amount reimbursed
- network_status_current: Current verification and active status

Data Quality Notes:
- Some insurance plans may have missing verification dates (handled as 'unverified')
- Employer information may be null for individual plans (handled with coalesce)
- Claim metrics are calculated from available data and may be zero for new plans

Performance Considerations:
- Indexed on insurance_plan_id for fast lookups
- Indexed on patient_id for patient-centric queries
- Indexed on carrier_id for carrier performance analysis
- Indexed on is_active for filtering active plans

Dependencies:
- int_insurance_coverage: Primary source for insurance plan data
- int_insurance_employer: Employer information for group plans
- int_claim_details: Claim data for performance metrics
- int_claim_payments: Payment data for reimbursement calculations
*/

-- 1. Source data retrieval
with source_insurance as (
    select * from {{ ref('int_insurance_coverage') }}
),

-- 2. Lookup/reference data
insurance_employer_lookup as (
    select 
        employer_id,
        employer_name,
        city as employer_city,
        state as employer_state
    from {{ ref('int_insurance_employer') }}
),

-- 3. Claim metrics calculation
claim_metrics as (
    select
        cd.insurance_plan_id,
        count(*) as total_claims,
        sum(case when cd.claim_status = 'approved' then 1 else 0 end) as approved_claims,
        sum(case when cd.claim_status = 'denied' then 1 else 0 end) as denied_claims,
        sum(case when cd.claim_status = 'pending' then 1 else 0 end) as pending_claims,
        sum(cd.billed_amount) as total_billed_amount,
        sum(cp.paid_amount) as total_paid_amount
    from {{ ref('int_claim_details') }} cd
    left join {{ ref('int_claim_payments') }} cp
        on cd.claim_id = cp.claim_id
    group by cd.insurance_plan_id
),

-- 4. Business logic enhancement
insurance_enhanced as (
    select
        -- Primary identification
        si.insurance_plan_id,

        -- Foreign keys
        si.carrier_id,
        si.employer_id,
        si.subscriber_id,
        si.patient_id,

        -- Business attributes
        si.plan_type,
        si.group_number,
        si.group_name,
        si.carrier_name,
        coalesce(iel.employer_name, si.employer_name) as employer_name,
        coalesce(iel.employer_city, si.employer_city) as employer_city,
        coalesce(iel.employer_state, si.employer_state) as employer_state,
        si.benefit_details,

        -- Status information
        si.is_active,
        si.is_incomplete_record,
        si.verification_date,

        -- Calculated fields - Network Status
        case
            when si.verification_date is null then 'unverified'
            when si.verification_date < current_date - interval '90 days' then 'expired'
            when si.is_active = true then 'active'
            else 'inactive'
        end as network_status_current,

        -- Calculated fields - Claim Performance Metrics
        coalesce(cm.total_claims, 0) as total_claims,
        coalesce(cm.approved_claims, 0) as approved_claims,
        coalesce(cm.denied_claims, 0) as denied_claims,
        coalesce(cm.pending_claims, 0) as pending_claims,
        coalesce(cm.total_billed_amount, 0) as total_billed_amount,
        coalesce(cm.total_paid_amount, 0) as total_paid_amount,

        -- Calculated fields - Enhanced Performance Metrics
        case
            when coalesce(cm.total_claims, 0) = 0 then 0
            else round((cm.approved_claims::numeric / cm.total_claims * 100)::numeric, 2)
        end as claim_approval_rate,
        
        case
            when coalesce(cm.total_billed_amount, 0) = 0 then 0
            else round((cm.total_paid_amount::numeric / cm.total_billed_amount * 100)::numeric, 2)
        end as average_reimbursement_rate,
        
        'unknown' as payment_velocity_score,  -- Payment timing data not available

        -- Metadata columns (with underscore prefix)
        si._created_at,
        si._updated_at,
        current_timestamp as _loaded_at
    from source_insurance si
    left join insurance_employer_lookup iel
        on si.employer_id = iel.employer_id
    left join claim_metrics cm
        on si.insurance_plan_id = cm.insurance_plan_id
),

-- 5. Final validation and filtering
final as (
    select * from insurance_enhanced
    where insurance_plan_id is not null
)

select * from final