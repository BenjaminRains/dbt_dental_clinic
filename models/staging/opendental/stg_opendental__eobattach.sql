{{
    config(
        materialized='incremental',
        unique_key='eob_attach_id',
        schema='staging'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'eobattach') }}
    {% if is_incremental() %}
        where "DateTCreated" > (select max(_created_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key and Foreign Keys
        {{ transform_id_columns([
            {'source': '"EobAttachNum"', 'target': 'eob_attach_id'},
            {'source': '"ClaimPaymentNum"', 'target': 'claim_payment_id'}
        ]) }},

        -- Attributes
        "FileName" as file_name,
        "RawBase64" as raw_base64,
        
        -- Date fields
        {{ clean_opendental_date('"DateTCreated"') }} as date_created,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTCreated"',
            updated_at_column='"DateTCreated"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
