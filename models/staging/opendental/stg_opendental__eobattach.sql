{{
    config(
        materialized='incremental',
        unique_key='eob_attach_id'
    )
}}

with source as (
    select * from {{ source('opendental', 'eobattach') }}
    {% if is_incremental() %}
        where "DateTCreated" > (select max(_created_at) from {{ this }})
    {% endif %}
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
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        "DateTCreated" as _created_at,
        "DateTCreated" as _updated_at  -- Using DateTCreated since EOB attachments are immutable

    from source
)

select * from renamed
