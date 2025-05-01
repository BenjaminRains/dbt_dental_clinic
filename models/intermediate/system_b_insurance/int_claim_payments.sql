{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='claim_id || "-" || procedure_id || "-" || claim_payment_id'
    )
}}

with ClaimProc as (
    select distinct
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

Final as (
    select
        -- Primary Key
        cp.claim_id,
        cp.procedure_id,
        cp.claim_payment_id,

        -- Financial Information
        cp.billed_amount,
        cp.allowed_amount,
        cp.paid_amount,
        cp.write_off as write_off_amount,
        cp.patient_responsibility,

        -- Payment Details
        cpy.check_amount,
        cpy.check_date,
        cpy.payment_type_id,
        cpy.is_partial,

        -- Meta Fields
        cpy.check_date as created_at,
        cpy.check_date as updated_at

    from ClaimProc cp
    left join ClaimPayment cpy
        on cp.claim_payment_id = cpy.claim_payment_id
)

select * from Final