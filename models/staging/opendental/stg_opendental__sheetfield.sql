{{ config(
    materialized = 'incremental',
    unique_key = 'sheet_field_id'
) }}

with source as (
    select sf.* 
    from {{ source('opendental', 'sheetfield') }} sf
    inner join {{ source('opendental', 'sheet') }} s 
        on sf."SheetNum" = s."SheetNum"
    where s."DateTimeSheet" >= '2023-01-01'
    {% if is_incremental() %}
        and s."DateTimeSheet" > (select max(sheet_datetime) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary Key
        "SheetFieldNum" as sheet_field_id,
        
        -- Foreign Keys
        "SheetNum" as sheet_id,
        "SheetFieldDefNum" as sheet_field_def_id,
        
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
        "ItemColor" as item_color,
        "DateTimeSig" as date_time_signature,
        "IsLocked" as is_locked,
        "TabOrderMobile" as tab_order_mobile,
        "UiLabelMobile" as ui_label_mobile,
        "UiLabelMobileRadioButton" as ui_label_mobile_radio_button,
        "CanElectronicallySign" as can_electronically_sign,
        "IsSigProvRestricted" as is_sig_prov_restricted

    from source
)

select * from renamed
