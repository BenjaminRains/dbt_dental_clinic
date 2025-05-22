{{ config(
    materialized='incremental',
    unique_key='insbluebooklog_id',
    incremental_strategy='delete+insert'
) }}

with source as (
    select * from {{ source('opendental', 'insbluebooklog') }}
    where "DateTEntry" >= '2023-01-01'  -- Following pattern from other staging models
    {% if is_incremental() %}
    and "DateTEntry" >= (select max(_created_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary Key
        "InsBlueBookLogNum" as insbluebooklog_id,
        
        -- Foreign Keys
        "ClaimProcNum" as claimprocedure_id,
        
        -- Numeric Fields
        "AllowedFee" as allowed_fee,
        
        -- String Fields
        "Description" as description,
        
        -- Timestamps
        "DateTEntry" as created_at,
        
        -- Required Metadata Columns
        current_timestamp as _loaded_at,
        "DateTEntry" as _created_at,
        "DateTEntry" as _updated_at
        
    from source
)

select * from renamed
