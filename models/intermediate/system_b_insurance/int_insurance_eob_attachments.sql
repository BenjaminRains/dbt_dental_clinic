{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='eob_attach_id'
    )
}}

/*
This model integrates EOB (Explanation of Benefits) attachments with claim payment data.
EOB attachments provide supporting documentation for insurance claim payments.

Key relationships:
- Each EOB attachment is related to a specific claim payment through claim_payment_id
- Multiple EOB attachments can exist for a single claim payment
*/

with EobAttach as (
    select
        -- Primary Key
        eob_attach_id,
        
        -- Foreign Keys
        claim_payment_id,
        
        -- Attributes
        created_at,
        file_name,
        raw_base64
    from {{ ref('stg_opendental__eobattach') }}
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
        eob.eob_attach_id,
        
        -- Foreign Keys
        eob.claim_payment_id,
        
        -- Attachment Details
        eob.file_name,
        eob.raw_base64,
        
        -- Payment Information (for context)
        cp.check_amount,
        cp.check_date,
        cp.payment_type_id,
        cp.is_partial,
        
        -- Meta Fields
        eob.created_at,
        eob.created_at as updated_at
    from EobAttach eob
    left join ClaimPayment cp
        on eob.claim_payment_id = cp.claim_payment_id
)

select * from Final