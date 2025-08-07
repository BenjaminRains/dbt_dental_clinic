{{ config(
    materialized='incremental',
    unique_key='insbluebooklog_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'insbluebooklog') }}
    where "DateTEntry" >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTEntry" > (select max(date_created) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"InsBlueBookLogNum"', 'target': 'insbluebooklog_id'},
            {'source': '"ClaimProcNum"', 'target': 'claim_procedure_id'}
        ]) }},
        
        -- Numeric Fields
        "AllowedFee" as allowed_fee,
        
        -- String Fields
        "Description" as description,
        
        -- Date Fields
        {{ clean_opendental_date('"DateTEntry"') }} as date_created,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTEntry"',
            updated_at_column='"DateTEntry"',
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
