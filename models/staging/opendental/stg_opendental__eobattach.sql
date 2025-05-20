{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'eobattach') }}
),

renamed as (
    select
        -- Primary Key
        "EobAttachNum" as eob_attach_id,
        
        -- Foreign Keys
        "ClaimPaymentNum" as claim_payment_id,
        
        -- Attributes
        "DateTCreated" as created_at,
        "FileName" as file_name,
        "RawBase64" as raw_base64,
        
        -- Metadata
        '{{ invocation_id }}' as _airbyte_ab_id,
        '{{ run_started_at }}' as _airbyte_emitted_at,
        '{{ invocation_id }}' as _airbyte_normalized_at,
        '{{ invocation_id }}' as _airbyte_eobattach_hashid
    from source
)

select * from renamed
