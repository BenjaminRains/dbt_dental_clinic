{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'insbluebooklog') }}
    where "DateTEntry" >= '2023-01-01'  -- Following pattern from other staging models
),

renamed as (
    select
        -- Primary Key
        "InsBlueBookLogNum" as insbluebooklog_num,
        
        -- Foreign Keys
        "ClaimProcNum" as claimprocedure_num,
        
        -- Numeric Fields
        "AllowedFee" as allowed_fee,
        
        -- String Fields
        "Description" as description,
        
        -- Timestamps
        "DateTEntry" as created_at
        
    from source
)

select * from renamed
