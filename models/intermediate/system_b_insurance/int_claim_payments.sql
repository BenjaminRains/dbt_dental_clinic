{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='claim_id || "-" || procedure_id || "-" || claim_payment_id'
    )
}}

with ClaimProc as (
    select
        claim_id,
        procedure_id,
        claim_payment_id,
        fee_billed as billed_amount,
        allowed_override as allowed_amount,
        insurance_payment_amount as paid_amount,
        write_off,
        copay_amount as patient_responsibility
    from {{ ref('stg_opendental__claimproc') }}
    where claim_payment_id is not null
),

ClaimPayment as (
    select
        claim_payment_id,
        check_amount,
        check_date,
        payment_type_id,
        is_partial
    from {{ ref('stg_opendental__claimpayment') }}
),

-- Add a CTE to get patient_id from claim_details
ClaimPatient as (
    select distinct
        claim_id,
        procedure_id,
        patient_id
    from {{ ref('int_claim_details') }}
),

-- Deduplicate at patient + claim_payment_id level
DeduplicatedClaims as (
    select
        cp.claim_id,
        cp.procedure_id,
        cp.claim_payment_id,
        cp.billed_amount,
        cp.allowed_amount,
        cp.paid_amount,
        cp.write_off,
        cp.patient_responsibility,
        row_number() over(partition by cpat.patient_id, cp.claim_payment_id order by cp.paid_amount desc) as rn
    from ClaimProc cp
    inner join ClaimPatient cpat
        on cp.claim_id = cpat.claim_id
        and cp.procedure_id = cpat.procedure_id
),

Final as (
    select
        -- Primary Key
        dc.claim_id,
        dc.procedure_id,
        dc.claim_payment_id,

        -- Financial Information
        dc.billed_amount,
        dc.allowed_amount,
        dc.paid_amount,
        dc.write_off as write_off_amount,
        dc.patient_responsibility,

        -- Payment Details
        cpy.check_amount,
        cpy.check_date,
        cpy.payment_type_id,
        cpy.is_partial,

        -- Meta Fields
        cpy.check_date as created_at,
        cpy.check_date as updated_at

    from DeduplicatedClaims dc
    left join ClaimPayment cpy
        on dc.claim_payment_id = cpy.claim_payment_id
    -- Only keep one record per patient + claim_payment_id combination
    where dc.rn = 1
)

select * from Final