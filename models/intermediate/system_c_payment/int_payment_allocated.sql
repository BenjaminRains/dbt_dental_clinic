{{ config(
    materialized='incremental',
    unique_key='payment_allocation_id',
    schema='intermediate'
) }}

/*
    Intermediate model for payment allocations
    Connects payments with their allocations across procedures and insurance claims
    Part of System C: Payment Allocation & Reconciliation
    
    This model combines:
    1. Patient payments and their allocations
    2. Insurance claim payments and their allocations
    3. Payment plan allocations
*/

WITH PaymentDefinitions AS (
    SELECT 
        definition_id,
        item_name,
        item_value,
        category_id
    FROM {{ ref('stg_opendental__definition') }}
    WHERE category_id = 1  -- Payment type categories only

    UNION ALL

    -- Add our business logic payment type descriptions
    SELECT 
        payment_type_id as definition_id,
        CASE 
            -- Patient payment types
            WHEN payment_type_id = 71 THEN 'Standard Payment'
            WHEN payment_type_id = 0 THEN 'Administrative'
            WHEN payment_type_id = 69 THEN 'High Value Payment'
            WHEN payment_type_id = 70 THEN 'Regular Payment'
            WHEN payment_type_id = 391 THEN 'High Value'
            WHEN payment_type_id = 412 THEN 'New Payment Type'
            WHEN payment_type_id = 72 THEN 'Refund'
            WHEN payment_type_id = 634 THEN 'New Type'
            WHEN payment_type_id = 574 THEN 'Very High Value'
            WHEN payment_type_id = 417 THEN 'Special Case'
            -- Insurance payment types
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
        (71), (0), (69), (70), (391), (412), (72), (634), (574), (417),  -- Patient payment types
        (261), (303), (465), (469), (466), (464)  -- Insurance payment types
    ) as t(payment_type_id)
),

PatientPayments AS (
    SELECT
        p.payment_id,
        p.patient_id,
        p.payment_date,
        COALESCE(ps.split_amount, p.payment_amount) as payment_amount,  -- Use split amount if available
        p.payment_type_id,
        p.check_number,
        p.bank_branch,
        p.payment_source,
        p.is_split_flag,
        p.is_recurring_cc_flag,
        p.payment_status,
        p.process_status,
        p.merchant_fee,
        p.payment_notes,
        p.clinic_id,
        p.created_by_user_id,
        p.entry_date,
        p.updated_at,
        p.deposit_id,
        p.external_id,
        p.is_cc_completed_flag,
        p.recurring_charge_date,
        p.receipt_text,
        ps.procedure_id,
        ps.procedure_date,
        proc.provider_id,
        ps.adjustment_id,  -- Add adjustment_id from paysplit
        ps.is_discount_flag,  -- Add discount flag from paysplit
        ps.discount_type,  -- Add discount type from paysplit
        ps.unearned_type  -- Add unearned type from paysplit
    FROM {{ ref('stg_opendental__payment') }} p
    LEFT JOIN {{ ref('stg_opendental__paysplit') }} ps
        ON p.payment_id = ps.payment_id
    LEFT JOIN {{ ref('stg_opendental__procedurelog') }} proc
        ON ps.procedure_id = proc.procedure_id
),

InsurancePayments AS (
    SELECT DISTINCT ON (ip.claim_payment_id)
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
        ip.clinic_id,
        ip.created_by_user_id,
        ip.created_date,
        ip.last_modified_at,
        ip.deposit_id,
        ip.note,
        cp.provider_id
    FROM {{ ref('stg_opendental__claimpayment') }} ip
    INNER JOIN {{ ref('stg_opendental__claimproc') }} cp
        ON ip.claim_payment_id = cp.claim_payment_id
        AND cp.status IN (1, 3)  -- Only include Received and Supplemental claims
        AND cp.claim_payment_id != 0  -- Exclude records with no payment
    WHERE ip.claim_payment_id IS NOT NULL
    ORDER BY ip.claim_payment_id, ip.check_date DESC
),

ClaimProcedures AS (
    SELECT DISTINCT
        claim_procedure_id,
        procedure_id,
        claim_id,
        patient_id,
        provider_id,
        plan_id,
        claim_payment_id,
        clinic_id,
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
    WHERE status IN (1, 3)  -- Only include Received and Supplemental claims
    AND claim_payment_id != 0  -- Exclude records with no payment
    AND COALESCE(insurance_finalized_date, claim_procedure_date) <= CURRENT_DATE  -- Exclude future-dated records
),

-- Combine all payment allocations
PaymentAllocations AS (
    -- Patient payments
    SELECT
        'PATIENT' AS payment_source_type,
        pp.payment_id,
        pp.patient_id,
        pp.clinic_id,
        pp.provider_id,
        pp.procedure_id,
        pp.adjustment_id,  -- Use adjustment_id from PatientPayments
        NULL AS payplan_id,
        NULL AS payplan_charge_id,
        pp.payment_amount AS split_amount,  -- This is now correctly using split amounts
        pp.payment_date,
        pp.procedure_date,
        pp.payment_type_id,
        pp.payment_source,
        pp.payment_status,
        pp.process_status,
        pp.is_discount_flag,  -- Use is_discount_flag from PatientPayments
        pp.discount_type,  -- Use discount_type from PatientPayments
        pp.unearned_type,  -- Use unearned_type from PatientPayments
        NULL AS payplan_debit_type,
        pp.merchant_fee,
        pp.payment_notes,
        pp.check_number,
        pp.bank_branch,
        pp.created_by_user_id,
        pp.entry_date,
        pp.updated_at,
        pp.deposit_id,
        pp.external_id,
        pp.is_cc_completed_flag,
        pp.recurring_charge_date,
        pp.receipt_text,
        NULL AS carrier_name,
        NULL AS is_partial,
        NULL AS payment_group_id,
        NULL AS insurance_subscriber_id,
        NULL AS claim_id,
        NULL AS plan_id,
        NULL AS deductible_applied,
        NULL AS write_off,
        NULL AS allowed_override,
        NULL AS copay_amount,
        NULL AS paid_other_insurance,
        NULL AS base_estimate,
        NULL AS insurance_estimate_total,
        NULL AS status,
        NULL AS percentage,
        NULL AS is_transfer,
        NULL AS is_overpay,
        NULL AS remarks,
        NULL AS code_sent,
        NULL AS estimate_note,
        pd.item_name AS payment_type_description,
        CASE
            WHEN pp.payment_date <= CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS include_in_ar,
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
    FROM PatientPayments pp
    LEFT JOIN PaymentDefinitions pd
        ON pp.payment_type_id = pd.definition_id

    UNION ALL

    -- Insurance claim payments
    SELECT
        'INSURANCE' AS payment_source_type,
        ip.claim_payment_id AS payment_id,
        cp.patient_id,
        cp.clinic_id,
        cp.provider_id,
        cp.procedure_id,
        NULL::INTEGER AS adjustment_id,
        NULL AS payplan_id,
        NULL AS payplan_charge_id,
        cp.insurance_payment_amount AS split_amount,
        COALESCE(cp.insurance_finalized_date, ip.check_date) AS payment_date,
        cp.procedure_date,
        ip.payment_type_id,
        NULL AS payment_source,
        NULL AS payment_status,
        NULL AS process_status,
        FALSE AS is_discount_flag,
        NULL AS discount_type,
        NULL AS unearned_type,
        NULL AS payplan_debit_type,
        NULL AS merchant_fee,
        ip.note AS payment_notes,
        ip.check_number,
        ip.bank_branch,
        ip.created_by_user_id,
        COALESCE(cp.claim_procedure_date, ip.created_date) AS entry_date,
        COALESCE(cp.last_modified_at, ip.last_modified_at) AS updated_at,
        ip.deposit_id,
        NULL AS external_id,
        NULL AS is_cc_completed_flag,
        NULL AS recurring_charge_date,
        NULL AS receipt_text,
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
        CASE
            WHEN COALESCE(cp.insurance_finalized_date, ip.check_date) <= CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS include_in_ar,
        CURRENT_TIMESTAMP AS model_created_at,
        CURRENT_TIMESTAMP AS model_updated_at
    FROM ClaimProcedures cp
    LEFT JOIN InsurancePayments ip 
        ON cp.claim_payment_id = ip.claim_payment_id
        AND cp.procedure_id = cp.procedure_id
    LEFT JOIN PaymentDefinitions pd
        ON ip.payment_type_id = pd.definition_id
)

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY payment_source_type, payment_id) AS payment_allocation_id,
    *
FROM PaymentAllocations 