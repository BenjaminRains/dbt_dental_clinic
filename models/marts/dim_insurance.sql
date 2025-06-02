{{
    config(
        materialized='table',
        schema='marts',
        unique_key='insurance_plan_id'
    )
}}

/*
Dimension table for insurance plans and carriers.
This model combines data from multiple sources to create a comprehensive
insurance dimension that supports both current and historical analysis.

Key features:
- Complete carrier and plan information
- Historical tracking of plan changes
- Benefit details and coverage rules
- Network status and verification tracking
- Employer information when applicable
- Enhanced performance metrics for BI analysis
*/

with InsuranceCoverage as (
    select * from {{ ref('int_insurance_coverage') }}
),

InsuranceEmployer as (
    select * from {{ ref('int_insurance_employer') }}
),

ClaimDetails as (
    select * from {{ ref('int_claim_details') }}
),

ClaimPayments as (
    select * from {{ ref('int_claim_payments') }}
),

ClaimMetrics as (
    select
        insurance_plan_id,
        count(*) as total_claims,
        sum(case when claim_status = 'approved' then 1 else 0 end) as approved_claims,
        sum(case when claim_status = 'denied' then 1 else 0 end) as denied_claims,
        sum(case when claim_status = 'pending' then 1 else 0 end) as pending_claims,
        sum(billed_amount) as total_billed_amount,
        sum(paid_amount) as total_paid_amount,
        avg(payment_time_days) as average_payment_time_days
    from ClaimDetails cd
    left join ClaimPayments cp
        on cd.claim_id = cp.claim_id
    group by insurance_plan_id
),

Final as (
    select
        -- Primary Key
        ic.insurance_plan_id,

        -- Foreign Keys
        ic.carrier_id,
        ic.employer_id,
        ic.subscriber_id,
        ic.patient_id,

        -- Plan Details
        ic.plan_type,
        ic.group_number,
        ic.group_name,
        ic.carrier_name,
        ic.employer_name,
        ic.employer_city,
        ic.employer_state,

        -- Benefit Details
        ic.benefit_details,

        -- Status Information
        ic.is_active,
        ic.is_incomplete_record,
        ic.hide_from_verify_list,
        ic.verification_date,

        -- Network Status (Derived Field)
        case
            when ic.verification_date is null then 'unverified'
            when ic.verification_date < current_date - interval '90 days' then 'expired'
            when ic.is_active = true then 'active'
            else 'inactive'
        end as network_status_current,

        -- Claim Performance Metrics (Derived Fields)
        coalesce(cm.total_claims, 0) as total_claims,
        coalesce(cm.approved_claims, 0) as approved_claims,
        coalesce(cm.denied_claims, 0) as denied_claims,
        coalesce(cm.pending_claims, 0) as pending_claims,
        coalesce(cm.total_billed_amount, 0) as total_billed_amount,
        coalesce(cm.total_paid_amount, 0) as total_paid_amount,
        coalesce(cm.average_payment_time_days, 0) as average_payment_time_days,

        -- Enhanced Performance Metrics (Derived Fields)
        case
            when coalesce(cm.total_claims, 0) = 0 then 0
            else round((cm.approved_claims::float / cm.total_claims) * 100, 2)
        end as claim_approval_rate,
        
        case
            when coalesce(cm.total_billed_amount, 0) = 0 then 0
            else round((cm.total_paid_amount::float / cm.total_billed_amount) * 100, 2)
        end as average_reimbursement_rate,
        
        case
            when cm.average_payment_time_days is null then 'unknown'
            when cm.average_payment_time_days <= 15 then 'excellent'
            when cm.average_payment_time_days <= 30 then 'good'
            when cm.average_payment_time_days <= 45 then 'average'
            else 'poor'
        end as payment_velocity_score,

        -- Metadata Columns (with underscore prefix)
        ic.created_at as _created_at,
        ic.updated_at as _updated_at,
        current_timestamp as _loaded_at
    from InsuranceCoverage ic
    left join InsuranceEmployer ie
        on ic.employer_id = ie.employer_id
    left join ClaimMetrics cm
        on ic.insurance_plan_id = cm.insurance_plan_id
)

select * from Final
