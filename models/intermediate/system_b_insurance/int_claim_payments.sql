{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key=['claim_id', 'procedure_id', 'claim_procedure_id', 'claim_payment_id']
    )
}}

WITH Claim as (
    select
        claim_id,
        patient_id
    from {{ ref('stg_opendental__claim') }}
),

ClaimProc as (
    select
        claim_id,
        procedure_id,
        claim_procedure_id,
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

EobAttachments as (
    select
        claim_payment_id,
        count(eob_attach_id) as eob_attachment_count,
        array_agg(eob_attach_id) as eob_attachment_ids,
        array_agg(file_name) as eob_attachment_file_names
    from {{ ref('stg_opendental__eobattach') }}
    group by claim_payment_id
),

-- Deduplicate at the source using the full composite key
DeduplicatedClaims as (
    select
        c.patient_id,
        cp.claim_id,
        cp.procedure_id,
        cp.claim_procedure_id,
        cp.claim_payment_id,
        cp.billed_amount,
        cp.allowed_amount,
        cp.paid_amount,
        cp.write_off,
        cp.patient_responsibility,
        row_number() over(
            partition by cp.claim_id, cp.procedure_id, cp.claim_procedure_id, cp.claim_payment_id
            order by cp.paid_amount desc
        ) as rn
    from ClaimProc cp
    inner join Claim c
        on cp.claim_id = c.claim_id
),

Final as (
    select
        -- Primary Key
        dc.claim_id,
        dc.procedure_id,
        dc.claim_procedure_id,
        dc.claim_payment_id,
        dc.patient_id, -- Include patient_id in the output

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

        -- EOB Attachment Information
        coalesce(eob.eob_attachment_count, 0) as eob_attachment_count,
        eob.eob_attachment_ids,
        eob.eob_attachment_file_names,
        
        -- Meta Fields
        cpy.check_date as created_at,
        cpy.check_date as updated_at

    from DeduplicatedClaims dc
    left join ClaimPayment cpy
        on dc.claim_payment_id = cpy.claim_payment_id
    left join EobAttachments eob
        on dc.claim_payment_id = eob.claim_payment_id
    where dc.rn = 1 -- Only keep one record per unique composite key combination
)

select * from Final