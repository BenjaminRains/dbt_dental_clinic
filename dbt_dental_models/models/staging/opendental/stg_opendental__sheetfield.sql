{{ config(
    materialized='incremental',
    unique_key='sheet_field_id'
) }}

with source_data as (
    select sf.*, s."DateTimeSheet", s."DateTSheetEdited"
    from {{ source('opendental', 'sheetfield') }} sf
    left join {{ source('opendental', 'sheet') }} s 
        on sf."SheetNum" = s."SheetNum"
    {% if is_incremental() %}
    where {{ clean_opendental_date('"DateTSheetEdited"') }} > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"SheetFieldNum"', 'target': 'sheet_field_id'},
            {'source': '"SheetNum"', 'target': 'sheet_id'},
            {'source': 'NULLIF("SheetFieldDefNum", 0)', 'target': 'sheet_field_def_id'}
        ]) }},
        
        -- Field Properties
        "FieldType"::integer as field_type,
        nullif(trim("FieldName"), '') as field_name,
        nullif(trim("FieldValue"), '') as field_value,
        "FontSize"::real as font_size,
        nullif(trim("FontName"), '') as font_name,
        "XPos"::integer as x_position,
        "YPos"::integer as y_position,
        "Width"::integer as width,
        "Height"::integer as height,
        "GrowthBehavior"::integer as growth_behavior,
        
        -- Radio Button Properties
        nullif(trim("RadioButtonValue"), '') as radio_button_value,
        nullif(trim("RadioButtonGroup"), '') as radio_button_group,
        
        -- Additional Properties
        "TabOrder"::integer as tab_order,
        nullif(trim("ReportableName"), '') as reportable_name,
        "TextAlign"::smallint as text_align,
        "ItemColor"::integer as item_color,
        "TabOrderMobile"::integer as tab_order_mobile,
        nullif(trim("UiLabelMobile"), '') as ui_label_mobile,
        nullif(trim("UiLabelMobileRadioButton"), '') as ui_label_mobile_radio_button,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"FontIsBold"') }} as is_font_bold,
        {{ convert_opendental_boolean('"IsRequired"') }} as is_required,
        {{ convert_opendental_boolean('"IsLocked"') }} as is_locked,
        {{ convert_opendental_boolean('"CanElectronicallySign"') }} as can_electronically_sign,
        {{ convert_opendental_boolean('"IsSigProvRestricted"') }} as is_sig_prov_restricted,
        
        -- Source Metadata
        {{ clean_opendental_date('"DateTimeSig"') }} as date_time_signature,
        {{ clean_opendental_date('"DateTSheetEdited"') }} as date_tstamp,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='coalesce("DateTimeSheet", "DateTimeSig")',
            updated_at_column='coalesce("DateTSheetEdited", "DateTimeSig")',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
