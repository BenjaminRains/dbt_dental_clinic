{{ config(
    materialized='table',
    schema='raw',
    tags=['etl', 'tracking']
) }}

with source as (
    select * from {{ source('opendental', 'etl_load_status') }}
),

renamed as (
    select
        -- Primary Key
        id,
        
        -- Tracking Fields
        table_name,
        last_extracted,
        rows_extracted,
        extraction_status,
        
        -- Metadata columns
        _loaded_at,
        _created_at,
        _updated_at
    from source
)

select * from renamed
