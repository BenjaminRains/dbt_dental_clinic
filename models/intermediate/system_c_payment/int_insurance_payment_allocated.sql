{{ config(
    materialized='incremental',
    unique_key='payment_allocation_id',
    schema='intermediate'
) }}

/*
    Intermediate model for insurance payment allocations
    Part of System C: Payment Allocation & Reconciliation
    
    This model handles:
    1. Insurance claim payments and their allocations
    2. Claim procedure details and provider assignments
    3. Insurance payment type definitions
    4. Payment group tracking for partial payments
*/

WITH
-- Get payment definitions with materialization
PaymentDefinitions AS MATERIALIZED (
    SELECT DISTINCT
        definition_id,
        item_name,
        item_value,
        category_id
    FROM {{ ref('stg_opendental__definition') }}
    WHERE category_id = 1

    UNION ALL

    SELECT DISTINCT
        payment_type_id as definition_id,
        CASE 
            WHEN payment_type_id = 261 THEN 'Insurance Check'
            WHEN payment_type_id = 303 THEN 'Insurance Electronic Payment'
            WHEN payment_type_id = 465 THEN 'Insurance Credit'
            WHEN payment_type_id = 469 THEN 'Insurance Check'
            WHEN payment_type_id = 466 THEN 'Insurance Electronic Payment'
            WHEN payment_type_id = 464 THEN 'Insurance Credit'
        END as item_name,
        NULL as item_value,
        1 as category_id
    FROM (VALUES 
        (261), (303), (465), (469), (466), (464)
    ) as t(payment_type_id)
),

-- Get insurance payments with materialization and filtering
InsurancePayments AS MATERIALIZED (
    SELECT DISTINCT
        ip.claim_payment_id,
        ip.check_date,
        ip.date_issued,
        ip.check_amount,
        ip.check_number,
        ip.bank_branch,
        ip.carrier_name,
        ip.is_partial,
        ip.payment_type_id,
        ip.payment_group_id,
        ip.created_by_user_id,
        ip.created_date,
        ip.last_modified_at,
        ip.deposit_id,
        ip.note
    FROM {{ ref('stg_opendental__claimpayment') }} ip
    WHERE ip.claim_payment_id IS NOT NULL
    AND COALESCE(ip.check_date, ip.date_issued) >= '2023-01-01'
),

-- Get claim procedures with materialization and filtering
ClaimProcedures AS MATERIALIZED (
    SELECT DISTINCT
        claim_procedure_id,
        procedure_id,
        claim_id,
        patient_id,
        provider_id,
        plan_id,
        claim_payment_id,
        insurance_subscriber_id,
        payment_plan_id,
        claim_procedure_date,
        procedure_date,
        insurance_finalized_date,
        deductible_applied,
        write_off,
        allowed_override,
        copay_amount,
        paid_other_insurance,
        base_estimate,
        insurance_estimate_total,
        insurance_payment_amount,
        status,
        percentage,
        is_transfer,
        is_overpay,
        remarks,
        code_sent,
        estimate_note,
        last_modified_at
    FROM {{ ref('stg_opendental__claimproc') }}
    WHERE status IN (1, 3)
    AND claim_payment_id != 0
    AND COALESCE(insurance_finalized_date, claim_procedure_date) <= CURRENT_DATE
    AND COALESCE(insurance_finalized_date, claim_procedure_date) >= '2023-01-01'
),

-- Get bluebook information for payment validation
BluebookInfo AS MATERIALIZED (
    SELECT
        ib.proc_id,
        ib.claim_id,
        ib.plan_id,
        ib.carrier_id,
        ib.insurance_payment_amount AS bluebook_payment_amount,
        ib.allowed_override_amount,
        ib.group_id,
        ib.claim_type,
        ibl.allowed_fee,
        ibl.description AS allowed_fee_description,
        ibl.created_at AS allowed_fee_updated_at
    FROM {{ ref('stg_opendental__insbluebook') }} ib
    LEFT JOIN {{ ref('stg_opendental__claimproc') }} cp
        ON ib.proc_id = cp.procedure_id
        AND ib.claim_id = cp.claim_id
        AND ib.plan_id = cp.plan_id
    LEFT JOIN {{ ref('stg_opendental__insbluebooklog') }} ibl
        ON cp.claim_procedure_id = ibl.claimprocedure_id
    WHERE ib.created_at >= '2023-01-01'
)

-- Final select with insurance payment allocations
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY ip.claim_payment_id, cp.procedure_id) AS payment_allocation_id,
    'INSURANCE' AS payment_source_type,
    ip.claim_payment_id AS payment_id,
    cp.patient_id,
    cp.provider_id,
    cp.procedure_id,
    NULL::INTEGER AS adjustment_id,
    NULL::INTEGER AS payplan_id,
    NULL::INTEGER AS payplan_charge_id,
    cp.insurance_payment_amount AS split_amount,
    COALESCE(cp.insurance_finalized_date, ip.check_date) AS payment_date,
    cp.procedure_date,
    ip.payment_type_id,
    NULL::TEXT AS payment_source,
    NULL::INTEGER AS payment_status,
    NULL::INTEGER AS process_status,
    FALSE AS is_discount_flag,
    NULL::INTEGER AS discount_type,
    NULL::INTEGER AS unearned_type,
    NULL::INTEGER AS payplan_debit_type,
    NULL::NUMERIC AS merchant_fee,
    ip.note AS payment_notes,
    ip.check_number,
    ip.bank_branch,
    ip.created_by_user_id,
    COALESCE(cp.claim_procedure_date, ip.created_date) AS entry_date,
    COALESCE(cp.last_modified_at, ip.last_modified_at) AS updated_at,
    ip.deposit_id,
    NULL::TEXT AS external_id,
    NULL::BOOLEAN AS is_cc_completed_flag,
    NULL::DATE AS recurring_charge_date,
    NULL::TEXT AS receipt_text,
    ip.carrier_name,
    ip.is_partial,
    ip.payment_group_id,
    cp.insurance_subscriber_id,
    cp.claim_id,
    cp.plan_id,
    cp.deductible_applied,
    cp.write_off,
    cp.allowed_override,
    cp.copay_amount,
    cp.paid_other_insurance,
    cp.base_estimate,
    cp.insurance_estimate_total,
    cp.status,
    cp.percentage,
    cp.is_transfer,
    cp.is_overpay,
    cp.remarks,
    cp.code_sent,
    cp.estimate_note,
    pd.item_name AS payment_type_description,
    -- Bluebook information
    bb.bluebook_payment_amount,
    bb.allowed_override_amount,
    bb.group_id,
    bb.claim_type,
    bb.allowed_fee,
    bb.allowed_fee_description,
    bb.allowed_fee_updated_at,
    CASE
        WHEN COALESCE(cp.insurance_finalized_date, ip.check_date) <= CURRENT_DATE THEN TRUE
        ELSE FALSE
    END AS include_in_ar,
    CURRENT_TIMESTAMP AS model_created_at,
    CURRENT_TIMESTAMP AS model_updated_at
FROM ClaimProcedures cp
INNER JOIN InsurancePayments ip 
    ON cp.claim_payment_id = ip.claim_payment_id
LEFT JOIN PaymentDefinitions pd
    ON ip.payment_type_id = pd.definition_id
LEFT JOIN BluebookInfo bb
    ON cp.procedure_id = bb.proc_id
    AND cp.claim_id = bb.claim_id
    AND cp.plan_id = bb.plan_id 