{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'claimform') }}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"ClaimFormNum"', 'target': 'claim_form_id'}
        ]) }},
        
        -- Business fields
        nullif(trim("Description"), '') as description,
        nullif(trim("FontName"), '') as font_name,
        "FontSize" as font_size,
        nullif(trim("UniqueID"), '') as unique_id,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"PrintImages"') }} as print_images,
        
        -- Layout/positioning fields
        "OffsetX" as offset_x,
        "OffsetY" as offset_y,
        "Width" as width,
        "Height" as height,
        
        -- Standardized metadata (includes _loaded_at from ETL pipeline)
        {{ standardize_metadata_columns() }}
        
    from source_data
)

select * from renamed_columns
