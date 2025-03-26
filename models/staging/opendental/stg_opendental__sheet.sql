with source as (
    select * from {{ source('opendental', 'sheet') }}
),

renamed as (
    select
        -- Primary Key
        "SheetNum" as sheet_num,
        
        -- Foreign Keys
        "PatNum" as patient_num,
        "SheetDefNum" as sheet_def_num,
        "DocNum" as doc_num,
        "ClinicNum" as clinic_num,
        "WebFormSheetID" as web_form_sheet_id,
        
        -- Timestamps
        "DateTimeSheet" as sheet_datetime,
        "DateTSheetEdited" as sheet_edited_datetime,
        
        -- Sheet Properties
        "SheetType" as sheet_type,
        "FontSize" as font_size,
        "FontName" as font_name,
        "Width" as width,
        "Height" as height,
        
        -- Flags
        CASE WHEN "IsLandscape" = 1 THEN true ELSE false END as is_landscape,
        CASE WHEN "ShowInTerminal" = 1 THEN true ELSE false END as show_in_terminal,
        CASE WHEN "IsWebForm" = 1 THEN true ELSE false END as is_web_form,
        CASE WHEN "IsMultiPage" = 1 THEN true ELSE false END as is_multi_page,
        CASE WHEN "IsDeleted" = 1 THEN true ELSE false END as is_deleted,
        CASE WHEN "HasMobileLayout" = 1 THEN true ELSE false END as has_mobile_layout,
        
        -- Additional Fields
        "InternalNote" as internal_note,
        "Description" as description,
        "RevID" as revision_id

    from source
    where "DateTimeSheet" >= '2023-01-01'
)

select * from renamed
