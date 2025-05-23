with source as (
    select 
        sfd.*,
        sd."DateTCreated",
        s."DateTimeSheet",
        s."DateTSheetEdited"
    from {{ source('opendental', 'sheetfielddef') }} sfd
    inner join {{ source('opendental', 'sheetdef') }} sd
        on sfd."SheetDefNum" = sd."SheetDefNum"
    left join {{ source('opendental', 'sheet') }} s
        on sd."SheetDefNum" = s."SheetDefNum"
),

renamed as (
    select
        -- Primary Key
        "SheetFieldDefNum" as sheet_field_def_id,
        
        -- Foreign Keys
        "SheetDefNum" as sheet_def_id,
        
        -- Regular Fields
        "FieldType" as field_type,
        "FieldName" as field_name,
        "FieldValue" as field_value,
        "FontSize" as font_size,
        "FontName" as font_name,
        "FontIsBold" as is_font_bold,
        "XPos" as x_position,
        "YPos" as y_position,
        "Width" as width,
        "Height" as height,
        "GrowthBehavior" as growth_behavior,
        "RadioButtonValue" as radio_button_value,
        "RadioButtonGroup" as radio_button_group,
        "IsRequired" as is_required,
        "TabOrder" as tab_order,
        "ReportableName" as reportable_name,
        "TextAlign" as text_align,
        "IsPaymentOption" as is_payment_option,
        "ItemColor" as item_color,
        "IsLocked" as is_locked,
        "TabOrderMobile" as tab_order_mobile,
        "UiLabelMobile" as ui_label_mobile,
        "UiLabelMobileRadioButton" as ui_label_mobile_radio_button,
        "LayoutMode" as layout_mode,
        "Language" as language,
        "CanElectronicallySign" as can_electronically_sign,
        "IsSigProvRestricted" as is_sig_prov_restricted,
        
        -- Metadata
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        coalesce("DateTimeSheet", "DateTCreated") as _created_at,   -- When the record was created in source
        coalesce("DateTSheetEdited", "DateTCreated") as _updated_at -- Last update timestamp

    from source
)

select * from renamed
