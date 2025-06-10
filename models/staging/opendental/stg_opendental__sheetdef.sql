{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'sheetdef') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"SheetDefNum"', 'target': 'sheet_def_id'},
            {'source': 'NULLIF("AutoCheckSaveImageDocCategory", 0)', 'target': 'auto_check_save_image_doc_category_id'}
        ]) }},
        
        -- Sheet Definition Properties
        nullif(trim("Description"), '') as description,
        "SheetType"::integer as sheet_type,
        "FontSize"::real as font_size,
        nullif(trim("FontName"), '') as font_name,
        "Width"::integer as width,
        "Height"::integer as height,
        "PageCount"::integer as page_count,
        "RevID"::integer as rev_id,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsLandscape"') }} as is_landscape,
        {{ convert_opendental_boolean('"IsMultiPage"') }} as is_multi_page,
        {{ convert_opendental_boolean('"BypassGlobalLock"') }} as bypass_global_lock,
        {{ convert_opendental_boolean('"HasMobileLayout"') }} as has_mobile_layout,
        {{ convert_opendental_boolean('"AutoCheckSaveImage"') }} as auto_check_save_image,
        
        -- Date fields using macro
        {{ clean_opendental_date('"DateTCreated"') }} as date_created,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTCreated"',
            updated_at_column='"DateTCreated"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
