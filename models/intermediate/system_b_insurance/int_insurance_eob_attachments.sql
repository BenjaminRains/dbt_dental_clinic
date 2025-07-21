{{ config(        materialized='table',
        
        unique_key='eob_attach_id') }}

/*
This model integrates EOB (Explanation of Benefits) attachments with claim payment data.
EOB attachments provide supporting documentation for insurance claim payments.

Key relationships:
- Each EOB attachment is related to a specific claim payment through claim_payment_id
- Multiple EOB attachments can exist for a single claim payment

Data limitations:
- The EOB attachments table contains data from 2020-2025
- The claim payments table only contains data from 2023-2025
- This model filters EOB attachments to 2023 and later to maintain referential integrity
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
    where created_at >= '2023-01-01' -- Filter to match claim payment date range
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
