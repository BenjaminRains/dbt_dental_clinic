{{ config(
    materialized='table',
    schema='intermediate',
    unique_key='patient_id'
) }}

/*
    Intermediate model for AR analysis
    Part of System D: AR Analysis
    
    This model:
    1. Aggregates procedure-level balances to patient level
    2. Provides comprehensive patient AR analysis
    3. Builds on int_ar_balance for consistency
    4. Uses shared calculations for payment and adjustment activity
    5. Includes patient demographic information
    6. Integrates with System B insurance models
*/

WITH PatientBalances AS (
    SELECT
        patient_id,
        SUM(current_balance) AS total_ar_balance,
        SUM(CASE WHEN aging_bucket = '0-30' THEN current_balance ELSE 0 END) AS balance_0_30_days,
        SUM(CASE WHEN aging_bucket = '31-60' THEN current_balance ELSE 0 END) AS balance_31_60_days,
        SUM(CASE WHEN aging_bucket = '61-90' THEN current_balance ELSE 0 END) AS balance_61_90_days,
        SUM(CASE WHEN aging_bucket = '90+' THEN current_balance ELSE 0 END) AS balance_over_90_days,
        SUM(insurance_pending_amount) AS estimated_insurance_ar,
        SUM(patient_responsibility) AS patient_responsibility,
        SUM(CASE WHEN responsible_party = 'INSURANCE' THEN current_balance ELSE 0 END) AS insurance_responsibility,
        MAX(last_payment_date) AS last_payment_date,
        COUNT(DISTINCT CASE WHEN current_balance > 0 THEN procedure_id END) AS open_procedures_count,
        COUNT(DISTINCT CASE WHEN claim_id IS NOT NULL THEN claim_id END) AS active_claims_count
    FROM {{ ref('int_ar_balance') }}
    GROUP BY 1
),

PaymentActivity AS (
    SELECT
        p.patient_id,
        -- Existing metrics
        COUNT(DISTINCT 
            CASE WHEN p.is_insurance_payment 
            THEN p.procedure_id END
        ) AS insurance_payment_count,
        COUNT(DISTINCT 
            CASE WHEN p.is_patient_payment 
            THEN p.procedure_id END
        ) AS patient_payment_count,
        COALESCE(SUM(
            CASE WHEN p.is_insurance_payment 
            THEN p.payment_amount ELSE 0 END
        ), 0) AS total_insurance_payments,
        COALESCE(SUM(
            CASE WHEN p.is_patient_payment 
            THEN p.payment_amount ELSE 0 END
        ), 0) AS total_patient_payments,
        MAX(
            CASE WHEN p.is_insurance_payment 
            THEN p.payment_date END
        ) AS last_insurance_payment_date,
        MAX(
            CASE WHEN p.is_patient_payment 
            THEN p.payment_date END
        ) AS last_patient_payment_date,
        -- New System C metrics
        COUNT(DISTINCT 
            CASE WHEN ps.split_type = 'NORMAL_PAYMENT' 
            THEN ps.paysplit_id END
        ) AS split_payment_count,
        COALESCE(SUM(ipa.merchant_fee), 0) AS total_merchant_fees,
        COUNT(DISTINCT ipa.payment_group_id) AS payment_group_count,
        MAX(ipa.payment_type_description) AS last_payment_type,
        MAX(ipa.check_number) AS last_check_number,
        MAX(ipa.bank_branch) AS last_bank_branch
    FROM {{ ref('int_ar_shared_calculations') }} p
    LEFT JOIN {{ ref('int_payment_split') }} ps
        ON p.procedure_id = ps.procedure_id
    LEFT JOIN {{ ref('int_insurance_payment_allocated') }} ipa
        ON p.procedure_id = ipa.procedure_id
    WHERE p.payment_date IS NOT NULL
    GROUP BY 1
),

AdjustmentActivity AS (
    SELECT
        patient_id,
        COUNT(DISTINCT CASE WHEN adjustment_type = 'WRITEOFF' THEN procedure_id END) AS writeoff_count,
        COUNT(DISTINCT CASE WHEN adjustment_type = 'DISCOUNT' THEN procedure_id END) AS discount_count,
        COALESCE(SUM(CASE WHEN adjustment_type = 'WRITEOFF' THEN adjustment_amount ELSE 0 END), 0) AS total_writeoffs,
        COALESCE(SUM(CASE WHEN adjustment_type = 'DISCOUNT' THEN adjustment_amount ELSE 0 END), 0) AS total_discounts,
        MAX(adjustment_date) AS last_adjustment_date
    FROM {{ ref('int_ar_shared_calculations') }}
    WHERE adjustment_date IS NOT NULL
    GROUP BY 1
),

-- Enhanced ClaimActivity CTE with System B details
ClaimActivity AS (
    SELECT
        cd.patient_id,
        -- Existing metrics
        COUNT(DISTINCT CASE 
            WHEN cd.claim_status = 'P' THEN cd.claim_id 
        END) AS pending_claims_count,
        COUNT(DISTINCT CASE 
            WHEN cd.claim_status = 'D' THEN cd.claim_id 
        END) AS denied_claims_count,
        COALESCE(SUM(CASE 
            WHEN cd.claim_status = 'P' 
            THEN cd.billed_amount - cd.paid_amount - cd.write_off_amount 
            ELSE 0 
        END), 0) AS pending_claims_amount,
        COALESCE(SUM(CASE 
            WHEN cd.claim_status = 'D' 
            THEN cd.billed_amount - cd.paid_amount - cd.write_off_amount 
            ELSE 0 
        END), 0) AS denied_claims_amount,
        MAX(CASE 
            WHEN cd.claim_status = 'P' 
            THEN cd.claim_date 
        END) AS last_pending_claim_date,
        MAX(CASE 
            WHEN cd.claim_status = 'D' 
            THEN cd.claim_date 
        END) AS last_denied_claim_date,
        -- New System B metrics
        COUNT(DISTINCT CASE 
            WHEN ct.entry_timestamp >= CURRENT_DATE - INTERVAL '30 days' 
            THEN ct.claim_id 
        END) AS recent_status_changes,
        MAX(ct.entry_timestamp) AS last_status_change_date,
        COALESCE(SUM(cp.paid_amount), 0) AS total_claim_payments,
        COUNT(DISTINCT cp.claim_payment_id) AS claim_payment_count,
        -- Additional claim validation
        COUNT(DISTINCT CASE 
            WHEN cd.claim_status = 'V' 
            THEN cd.claim_id 
        END) AS verified_claims_count,
        MAX(cd.verification_date) AS last_claim_verification,
        -- Benefit utilization
        COALESCE(SUM(
            CASE 
                WHEN cd.benefit_details IS NOT NULL 
                THEN (cd.benefit_details->>'monetary_amount')::numeric 
                ELSE 0 
            END
        ), 0) AS total_benefits_used,
        COALESCE(SUM(
            CASE 
                WHEN cd.benefit_details IS NOT NULL 
                THEN (cd.benefit_details->>'remaining_amount')::numeric 
                ELSE 0 
            END
        ), 0) AS total_benefits_remaining
    FROM {{ ref('int_claim_details') }} cd
    LEFT JOIN {{ ref('int_claim_tracking') }} ct
        ON cd.claim_id = ct.claim_id
    LEFT JOIN {{ ref('int_claim_payments') }} cp
        ON cd.claim_id = cp.claim_id
    GROUP BY 1
),

-- Base patient information
BasePatientInfo AS (
    SELECT
        p.patient_id,
        p.preferred_name,
        p.middle_initial,
        p.patient_status,
        p.position_code,
        p.birth_date,
        p.first_visit_date,
        p.has_insurance_flag,
        GREATEST(
            COALESCE(MAX(ha.appointment_datetime), 
                '1900-01-01'::timestamp),
            COALESCE(MAX(pl.procedure_date), 
                '1900-01-01'::date)
        ) as last_visit_date,
        -- Procedure counts with insurance
        COUNT(DISTINCT CASE 
            WHEN pl.procedure_status = 2 
                AND pl.billing_type_one_id IS NOT NULL 
            THEN pl.procedure_id 
        END) as procedures_with_primary_insurance,
        COUNT(DISTINCT CASE 
            WHEN pl.procedure_status = 2 
                AND pl.billing_type_two_id IS NOT NULL 
            THEN pl.procedure_id 
        END) as procedures_with_secondary_insurance
    FROM {{ ref('stg_opendental__patient') }} p
    LEFT JOIN {{ ref('stg_opendental__histappointment') }} ha 
        ON p.patient_id = ha.patient_id
    LEFT JOIN {{ ref('stg_opendental__procedurelog') }} pl 
        ON p.patient_id = pl.patient_id
    WHERE p.patient_status != 5  -- Exclude deleted patients
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

-- Insurance coverage information
InsuranceCoverage AS (
    SELECT
        patient_id,
        MAX(CASE WHEN ordinal = 1 THEN insurance_plan_id END) as primary_insurance_id,
        MAX(CASE WHEN ordinal = 2 THEN insurance_plan_id END) as secondary_insurance_id,
        MAX(CASE WHEN ordinal = 1 THEN group_name END) as primary_insurance_group,
        MAX(CASE WHEN ordinal = 2 THEN group_name END) as secondary_insurance_group,
        MAX(CASE WHEN ordinal = 1 THEN plan_type END) as primary_insurance_type,
        MAX(CASE WHEN ordinal = 2 THEN plan_type END) as secondary_insurance_type,
        MAX(CASE WHEN ordinal = 1 THEN effective_date END) as coverage_start_date,
        MAX(CASE WHEN ordinal = 1 THEN termination_date END) as coverage_end_date,
        MAX(CASE WHEN ordinal = 1 THEN benefit_details->>'coverage_percent' END) as benefit_percentage,
        MAX(CASE WHEN ordinal = 1 THEN benefit_details->>'deductible_met' END) as deductible_met,
        MAX(CASE WHEN ordinal = 1 THEN benefit_details->>'deductible_remaining' END) as deductible_remaining,
        MAX(CASE WHEN ordinal = 1 THEN benefit_details->>'annual_max_met' END) as annual_max_met,
        MAX(CASE WHEN ordinal = 1 THEN benefit_details->>'annual_max_remaining' END) as annual_max_remaining
    FROM {{ ref('int_insurance_coverage') }}
    GROUP BY patient_id
),

-- Final Patient Info with all details
PatientInfo AS (
    SELECT
        bpi.patient_id,
        bpi.preferred_name,
        bpi.middle_initial,
        bpi.patient_status,
        bpi.position_code,
        bpi.birth_date,
        bpi.first_visit_date,
        bpi.has_insurance_flag,
        bpi.last_visit_date,
        bpi.procedures_with_primary_insurance,
        bpi.procedures_with_secondary_insurance,
        ic.primary_insurance_id,
        ic.secondary_insurance_id,
        ic.primary_insurance_group,
        ic.secondary_insurance_group,
        ic.primary_insurance_type,
        ic.secondary_insurance_type,
        ic.coverage_start_date,
        ic.coverage_end_date,
        ic.benefit_percentage,
        ic.deductible_met,
        ic.deductible_remaining,
        ic.annual_max_met,
        ic.annual_max_remaining,
        CASE 
            WHEN bpi.position_code = 0 THEN 'Regular Patient'
            WHEN bpi.position_code = 1 THEN 'House Account'
            WHEN bpi.position_code = 2 THEN 'Staff Member'
            WHEN bpi.position_code = 3 THEN 'VIP Patient'
            WHEN bpi.position_code = 4 THEN 'Other'
            ELSE 'Unknown'
        END AS patient_category
    FROM BasePatientInfo bpi
    LEFT JOIN InsuranceCoverage ic
        ON bpi.patient_id = ic.patient_id
)

SELECT
    pb.patient_id,
    pb.total_ar_balance,
    pb.balance_0_30_days,
    pb.balance_31_60_days,
    pb.balance_61_90_days,
    pb.balance_over_90_days,
    pb.estimated_insurance_ar,
    pb.patient_responsibility,
    pb.insurance_responsibility,
    pb.last_payment_date,
    pb.open_procedures_count,
    pb.active_claims_count,
    pa.insurance_payment_count,
    pa.patient_payment_count,
    pa.total_insurance_payments,
    pa.total_patient_payments,
    pa.last_insurance_payment_date,
    pa.last_patient_payment_date,
    pa.split_payment_count,
    pa.total_merchant_fees,
    pa.payment_group_count,
    pa.last_payment_type,
    pa.last_check_number,
    pa.last_bank_branch,
    aa.writeoff_count,
    aa.discount_count,
    aa.total_writeoffs,
    aa.total_discounts,
    aa.last_adjustment_date,
    ca.pending_claims_count,
    ca.denied_claims_count,
    ca.pending_claims_amount,
    ca.denied_claims_amount,
    ca.last_pending_claim_date,
    ca.last_denied_claim_date,
    ca.recent_status_changes,
    ca.last_status_change_date,
    ca.total_claim_payments,
    ca.claim_payment_count,
    ca.verified_claims_count,
    ca.last_claim_verification,
    ca.total_benefits_used,
    ca.total_benefits_remaining,
    pi.preferred_name,
    pi.middle_initial,
    pi.patient_status,
    pi.patient_category,
    pi.birth_date,
    pi.first_visit_date,
    pi.last_visit_date,
    pi.has_insurance_flag,
    pi.procedures_with_primary_insurance,
    pi.procedures_with_secondary_insurance,
    pi.primary_insurance_id,
    pi.secondary_insurance_id,
    pi.primary_insurance_group,
    pi.secondary_insurance_group,
    pi.primary_insurance_type,
    pi.secondary_insurance_type,
    pi.coverage_start_date,
    pi.coverage_end_date,
    pi.benefit_percentage,
    pi.deductible_met,
    pi.deductible_remaining,
    pi.annual_max_met,
    pi.annual_max_remaining,
    CURRENT_TIMESTAMP as model_created_at,
    CURRENT_TIMESTAMP as model_updated_at
FROM PatientBalances pb
LEFT JOIN PaymentActivity pa
    ON pb.patient_id = pa.patient_id
LEFT JOIN AdjustmentActivity aa
    ON pb.patient_id = aa.patient_id
LEFT JOIN ClaimActivity ca
    ON pb.patient_id = ca.patient_id
LEFT JOIN PatientInfo pi
    ON pb.patient_id = pi.patient_id
WHERE pi.patient_id IS NOT NULL 
  AND pi.patient_status != 5  -- Exclude deleted patients