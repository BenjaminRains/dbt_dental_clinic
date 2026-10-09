with source as (
    select * from {{ source('opendental', 'sheet') }}
),

renamed as (
    select
        -- Primary key
        SheetNum as sheet_id,

        -- Relationships
        PatNum as patient_id,
        SheetDefNum as sheet_definition_id,
        DocNum as document_id,
        ClinicNum as clinic_id,
        WebFormSheetID as web_form_sheet_id,

        -- Sheet type and description
        SheetType as sheet_type,
        Description as sheet_description,

        -- Format configuration
        FontSize as font_size,
        FontName as font_name,
        Width as width,
        Height as height,
        IsLandscape as is_landscape_flag,

        -- Display settings
        ShowInTerminal as show_in_terminal_flag,
        IsWebForm as is_web_form_flag,
        IsMultiPage as is_multi_page_flag,
        HasMobileLayout as has_mobile_layout_flag,

        -- Status flags
        IsDeleted as is_deleted_flag,

        -- Version control
        RevID as revision_id,

        -- Notes
        InternalNote as internal_notes,

        -- Dates
        DateTimeSheet as sheet_datetime,
        DateTSheetEdited as last_edited_datetime
    from source
)

select * from renamed