{{ config(        
    materialized='table',
    unique_key='eob_attach_id',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_claim_payment_id_idx ON {{ this }} (claim_payment_id)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_check_date_idx ON {{ this }} (check_date)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_payment_type_id_idx ON {{ this }} (payment_type_id)",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_is_partial_idx ON {{ this }} (is_partial) WHERE is_partial = true",
        "CREATE INDEX IF NOT EXISTS {{ this.name }}_claim_payment_check_date_idx ON {{ this }} (claim_payment_id, check_date)"
    ]
) }}

/*
This model integrates EOB (Explanation of Benefits) attachments with claim payment data.
EOB attachments provide supporting documentation for insurance claim payments.

Key relationships:
- Each EOB attachment is related to a specific claim payment through claim_payment_id
- Multiple EOB attachments can exist for a single claim payment

Primary Source: stg_opendental__eobattach (preserves EOB attachment metadata)
Secondary Source: stg_opendental__claimpayment (for payment context only)

Data limitations:
- The EOB attachments table contains data from 2020-2025
- The claim payments table only contains data from 2023-2025
- This model filters EOB attachments to 2023 and later to maintain referential integrity

Note: EOB attachment table does not have user creation tracking (_created_by)
      Only business timestamps (_created_at, _updated_at) and pipeline metadata are available
*/

with EobAttach as (
    select
        -- Primary Key
        eob_attach_id,
        
        -- Foreign Keys
        claim_payment_id,
        
        -- Attributes
        file_name,
        raw_base64,
        
        -- Metadata (preserved from primary source)
        _loaded_at,
        _transformed_at,
        _created_at,
        _updated_at
    from {{ ref('stg_opendental__eobattach') }}
    where _created_at >= '2023-01-01' -- Filter to match claim payment date range
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
        
        -- Primary source metadata (EOB attachment - only available fields)
        {{ standardize_intermediate_metadata(
            primary_source_alias='eob',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']
        ) }}
        
    from EobAttach eob
    left join ClaimPayment cp
        on eob.claim_payment_id = cp.claim_payment_id
)

select * from Final
