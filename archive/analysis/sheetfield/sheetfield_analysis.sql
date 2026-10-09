with source as (
    select * from {{ source('opendental', 'sheetfield') }}
),

renamed as (
    select
        -- Primary key
        SheetFieldNum as sheet_field_id,

        -- Relationships
        SheetNum as sheet_id,
        SheetFieldDefNum as field_definition_id,

        -- Field identification
        FieldType as field_type,
        FieldName as field_name,
        FieldValue as field_value,
        ReportableName as reportable_name,

        -- Visual formatting
        FontSize as font_size,
        FontName as font_name,
        FontIsBold as is_bold_flag,
        TextAlign as text_alignment,
        ItemColor as item_color,

        -- Position and size
        XPos as x_position,
        YPos as y_position,
        Width as width,
        Height as height,
        GrowthBehavior as growth_behavior,

        -- Radio button configuration
        RadioButtonValue as radio_button_value,
        RadioButtonGroup as radio_button_group,

        -- Form behavior
        IsRequired as is_required_flag,
        IsLocked as is_locked_flag,
        TabOrder as tab_order,

        -- Mobile configuration
        TabOrderMobile as mobile_tab_order,
        UiLabelMobile as mobile_label,
        UiLabelMobileRadioButton as mobile_radio_button_label,

        -- Electronic signature
        DateTimeSig as signature_datetime,
        CanElectronicallySign as can_sign_electronically_flag,
        IsSigProvRestricted as is_signature_provider_restricted_flag
    from source
)

select * from renamed