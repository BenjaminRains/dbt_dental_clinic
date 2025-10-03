{{ config(
    materialized='incremental',
    unique_key='eob_attach_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'eobattach') }}
    {% if is_incremental() %}
        where {{ clean_opendental_date('"DateTCreated"') }} > (select max(_created_at) from {{ this }})
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
        
        -- Source system timestamp columns (when available)
        {{ clean_opendental_date('"DateTCreated"') }} as _created_at,
        {{ clean_opendental_date('"DateTCreated"') }} as _updated_at,
        
        -- Standardized metadata columns (ETL and dbt tracking)
        {{ standardize_metadata_columns() }}

    from source_data
)

select * from renamed_columns
