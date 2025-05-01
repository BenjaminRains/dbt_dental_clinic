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

-- Get all active patients with balances to ensure complete coverage
ActivePatients AS (
    SELECT
        patient_id,
        patient_status,
        total_balance
    FROM {{ ref('int_patient_profile') }}
    WHERE patient_status IN (0, 1, 2, 3)
),

PaymentActivity AS (
    -- First get the payments at their source, before joins can multiply them
    WITH PatientPaymentTotals AS (
        SELECT
            patient_id,
            COUNT(DISTINCT procedure_id) AS payment_count,
            COALESCE(SUM(split_amount), 0) AS total_payments,
            MAX(payment_date) AS last_payment_date
        FROM {{ ref('int_patient_payment_allocated') }}
        WHERE include_in_ar = TRUE
        GROUP BY patient_id
    ),
    InsurancePaymentTotals AS (
        SELECT
            patient_id,
            COUNT(DISTINCT procedure_id) AS payment_count,
            COALESCE(SUM(split_amount), 0) AS total_payments,
            MAX(payment_date) AS last_payment_date
        FROM {{ ref('int_insurance_payment_allocated') }}
        WHERE include_in_ar = TRUE
        GROUP BY patient_id
    ),
    SystemCMetrics AS (
        SELECT
            ps.patient_id,
            CASE
                WHEN COUNT(DISTINCT ipa.payment_group_id) = 0 
                THEN 0
                ELSE COUNT(DISTINCT CASE 
                    WHEN ps.split_type = 'NORMAL_PAYMENT' 
                    THEN ps.paysplit_id 
                END)
            END AS split_payment_count,
            COUNT(DISTINCT ipa.payment_group_id) AS payment_group_count,
            SUM(COALESCE(ps.merchant_fee, 0)) AS total_merchant_fees,
            MAX(ps.payment_type) AS last_payment_type,
            MAX(ps.check_number) AS last_check_number,
            MAX(ps.bank_branch) AS last_bank_branch
            -- Additional metrics can be added here
        FROM {{ ref('int_payment_split') }} ps
        LEFT JOIN {{ ref('int_insurance_payment_allocated') }} ipa
            ON ps.payment_id = ipa.payment_id
        GROUP BY ps.patient_id
    )
    SELECT
        ap.patient_id,
        COALESCE(ipt.payment_count, 0) AS insurance_payment_count,
        COALESCE(ppt.payment_count, 0) AS patient_payment_count,
        COALESCE(ipt.total_payments, 0) AS total_insurance_payments,
        COALESCE(ppt.total_payments, 0) AS total_patient_payments,
        ipt.last_payment_date AS last_insurance_payment_date,
        ppt.last_payment_date AS last_patient_payment_date,
        COALESCE(scm.split_payment_count, 0) AS split_payment_count,
        COALESCE(scm.total_merchant_fees, 0) AS total_merchant_fees,
        COALESCE(scm.payment_group_count, 0) AS payment_group_count,
        scm.last_payment_type,
        scm.last_check_number,
        scm.last_bank_branch
    FROM ActivePatients ap
    LEFT JOIN PatientPaymentTotals ppt
        ON ap.patient_id = ppt.patient_id
    LEFT JOIN InsurancePaymentTotals ipt
        ON ap.patient_id = ipt.patient_id
    LEFT JOIN SystemCMetrics scm
        ON ap.patient_id = scm.patient_id
),

AdjustmentActivity AS (
    -- Calculate adjustments directly from source
    SELECT
        patient_id,
        COUNT(DISTINCT CASE WHEN adjustment_category = 'insurance_writeoff' THEN adjustment_id END) AS writeoff_count,
        COUNT(DISTINCT CASE WHEN adjustment_category LIKE '%discount%' THEN adjustment_id END) AS discount_count,
        COALESCE(SUM(CASE WHEN adjustment_category = 'insurance_writeoff' THEN adjustment_amount ELSE 0 END), 0) AS total_writeoffs,
        COALESCE(SUM(CASE WHEN adjustment_category LIKE '%discount%' THEN adjustment_amount ELSE 0 END), 0) AS total_discounts,
        MAX(adjustment_date) AS last_adjustment_date
    FROM {{ ref('int_adjustments') }}
    GROUP BY patient_id
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
        -- Use the source system first_visit_date
        p.first_visit_date,
        -- Calculate last_visit_date as the latest of appointment or procedure dates
        GREATEST(
            COALESCE(MAX(CASE 
                WHEN ha.appointment_datetime <= CURRENT_TIMESTAMP 
                THEN ha.appointment_datetime::date
                ELSE NULL 
            END), '1900-01-01'::date),
            COALESCE(MAX(CASE 
                WHEN pl.procedure_date <= CURRENT_DATE 
                THEN pl.procedure_date 
                ELSE NULL 
            END), '1900-01-01'::date)
        ) as last_visit_date,
        p.has_insurance_flag,
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
    WHERE p.patient_status IN (0, 1, 2, 3)  -- Only include active patients
    GROUP BY p.patient_id, p.preferred_name, p.middle_initial, p.patient_status, p.position_code,
     p.birth_date, p.has_insurance_flag, p.first_visit_date
),

-- Insurance coverage information
InsuranceCoverage AS (
    SELECT
        ic.patient_id,
        MAX(CASE 
            WHEN ic.ordinal = 1 
                AND ip.hide_from_verify_list = false
                AND ic.is_active = true
            THEN ic.insurance_plan_id 
            ELSE NULL
        END) as primary_insurance_id,
        MAX(CASE 
            WHEN ic.ordinal = 2 
                AND (ip.hide_from_verify_list = false OR ip.hide_from_verify_list IS NULL)
                AND ic.is_active = true
            THEN ic.insurance_plan_id 
            ELSE NULL
        END) as secondary_insurance_id,
        MAX(CASE WHEN ic.ordinal = 1 THEN ic.group_name END) as primary_insurance_group,
        MAX(CASE WHEN ic.ordinal = 2 THEN ic.group_name END) as secondary_insurance_group,
        MAX(CASE WHEN ic.ordinal = 1 THEN ic.plan_type END) as primary_insurance_type,
        MAX(CASE WHEN ic.ordinal = 2 THEN ic.plan_type END) as secondary_insurance_type,
        MAX(CASE WHEN ic.ordinal = 1 THEN ic.effective_date END) as coverage_start_date,
        MAX(CASE WHEN ic.ordinal = 1 THEN ic.termination_date END) as coverage_end_date,
        MAX(CASE WHEN ic.ordinal = 1 THEN ic.benefit_details->>'coverage_percent' END) as benefit_percentage,
        MAX(CASE WHEN ic.ordinal = 1 THEN ic.benefit_details->>'deductible_met' END) as deductible_met,
        MAX(CASE WHEN ic.ordinal = 1 THEN ic.benefit_details->>'deductible_remaining' END) as deductible_remaining,
        MAX(CASE WHEN ic.ordinal = 1 THEN ic.benefit_details->>'annual_max_met' END) as annual_max_met,
        MAX(CASE WHEN ic.ordinal = 1 THEN ic.benefit_details->>'annual_max_remaining' END) as annual_max_remaining
    FROM {{ ref('int_insurance_coverage') }} ic
    LEFT JOIN {{ ref('stg_opendental__insplan') }} ip
        ON ic.insurance_plan_id = ip.insurance_plan_id
    GROUP BY ic.patient_id
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
        -- Ensure first_visit_date is never after last_visit_date
        CASE
            WHEN bpi.first_visit_date > bpi.last_visit_date THEN bpi.last_visit_date
            ELSE bpi.first_visit_date
        END as first_visit_date,
        bpi.last_visit_date,
        bpi.has_insurance_flag,
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

-- Start with all active patients as the base
SELECT
    ap.patient_id,
    COALESCE(pb.total_ar_balance, 0) AS total_ar_balance,
    COALESCE(pb.balance_0_30_days, 0) AS balance_0_30_days,
    COALESCE(pb.balance_31_60_days, 0) AS balance_31_60_days,
    COALESCE(pb.balance_61_90_days, 0) AS balance_61_90_days,
    COALESCE(pb.balance_over_90_days, 0) AS balance_over_90_days,
    COALESCE(pb.estimated_insurance_ar, 0) AS estimated_insurance_ar,
    COALESCE(pb.patient_responsibility, 0) AS patient_responsibility,
    COALESCE(pb.insurance_responsibility, 0) AS insurance_responsibility,
    pb.last_payment_date,
    COALESCE(pb.open_procedures_count, 0) AS open_procedures_count,
    COALESCE(pb.active_claims_count, 0) AS active_claims_count,
    COALESCE(pa.insurance_payment_count, 0) AS insurance_payment_count,
    COALESCE(pa.patient_payment_count, 0) AS patient_payment_count,
    COALESCE(pa.total_insurance_payments, 0) AS total_insurance_payments,
    COALESCE(pa.total_patient_payments, 0) AS total_patient_payments,
    pa.last_insurance_payment_date,
    pa.last_patient_payment_date,
    COALESCE(pa.split_payment_count, 0) AS split_payment_count,
    COALESCE(pa.total_merchant_fees, 0) AS total_merchant_fees,
    COALESCE(pa.payment_group_count, 0) AS payment_group_count,
    pa.last_payment_type,
    pa.last_check_number,
    pa.last_bank_branch,
    COALESCE(aa.writeoff_count, 0) AS writeoff_count,
    COALESCE(aa.discount_count, 0) AS discount_count,
    COALESCE(aa.total_writeoffs, 0) AS total_writeoffs,
    COALESCE(aa.total_discounts, 0) AS total_discounts,
    aa.last_adjustment_date,
    COALESCE(ca.pending_claims_count, 0) AS pending_claims_count,
    COALESCE(ca.denied_claims_count, 0) AS denied_claims_count,
    COALESCE(ca.pending_claims_amount, 0) AS pending_claims_amount,
    COALESCE(ca.denied_claims_amount, 0) AS denied_claims_amount,
    ca.last_pending_claim_date,
    ca.last_denied_claim_date,
    COALESCE(ca.recent_status_changes, 0) AS recent_status_changes,
    ca.last_status_change_date,
    COALESCE(ca.total_claim_payments, 0) AS total_claim_payments,
    COALESCE(ca.claim_payment_count, 0) AS claim_payment_count,
    COALESCE(ca.verified_claims_count, 0) AS verified_claims_count,
    ca.last_claim_verification,
    COALESCE(ca.total_benefits_used, 0) AS total_benefits_used,
    COALESCE(ca.total_benefits_remaining, 0) AS total_benefits_remaining,
    pi.preferred_name,
    pi.middle_initial,
    pi.patient_status,
    pi.patient_category,
    pi.birth_date,
    pi.first_visit_date,
    pi.last_visit_date,
    pi.has_insurance_flag,
    COALESCE(pi.procedures_with_primary_insurance, 0) AS procedures_with_primary_insurance,
    COALESCE(pi.procedures_with_secondary_insurance, 0) AS procedures_with_secondary_insurance,
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
FROM ActivePatients ap
LEFT JOIN PatientBalances pb
    ON ap.patient_id = pb.patient_id
LEFT JOIN PaymentActivity pa
    ON ap.patient_id = pa.patient_id
LEFT JOIN AdjustmentActivity aa
    ON ap.patient_id = aa.patient_id
LEFT JOIN ClaimActivity ca
    ON ap.patient_id = ca.patient_id
LEFT JOIN PatientInfo pi
    ON ap.patient_id = pi.patient_id
WHERE ap.patient_status IN (0, 1, 2, 3)  -- Only include active patients