{{ config(
    materialized='table',
    schema='raw',
    tags=['etl', 'tracking']
) }}

with source as (
    select * from {{ source('opendental', 'etl_transform_status') }}
),

renamed as (
    select
        -- Primary Key
        id,
        
        -- Tracking Fields
        table_name,
        last_transformed,
        rows_transformed,
        transformation_status,
        
        -- Metadata columns
        _loaded_at,
        _created_at,
        _updated_at
    from source
)

select * from renamed
