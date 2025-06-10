{{ config(
    materialized='incremental',
    unique_key='insbluebooklog_id',
    incremental_strategy='delete+insert'
) }}

with source_data as (
    select * from {{ source('opendental', 'insbluebooklog') }}
    where "DateTEntry" >= '2023-01-01'  -- Following pattern from other staging models
    {% if is_incremental() %}
    and "DateTEntry" >= (select max(_created_at) from {{ this }})
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
