with source as (
    select * from {{ source('opendental', 'sheetdef') }}
),

renamed as (
    select
        -- Primary Key
        "SheetDefNum" as sheet_def_id,
        
        -- Attributes
        "Description" as description,
        "SheetType" as sheet_type,
        "FontSize" as font_size,
        "FontName" as font_name,
        "Width" as width,
        "Height" as height,
        "IsLandscape" as is_landscape,
        "PageCount" as page_count,
        "IsMultiPage" as is_multi_page,
        "BypassGlobalLock" as bypass_global_lock,
        "HasMobileLayout" as has_mobile_layout,
        "RevID" as rev_id,
        "AutoCheckSaveImage" as auto_check_save_image,
        "AutoCheckSaveImageDocCategory" as auto_check_save_image_doc_category,
        
        -- Metadata
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        "DateTCreated" as _created_at,   -- When the record was created in source
        "DateTCreated" as _updated_at    -- Last update timestamp (using creation date since no update timestamp available)

    from source
)

select * from renamed
