{{ config(
    materialized='incremental',
    unique_key='sheet_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'sheet') }}
    where "DateTimeSheet" >= '2023-01-01'
    {% if is_incremental() %}
        and "DateTSheetEdited" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"SheetNum"', 'target': 'sheet_id'},
            {'source': 'NULLIF("PatNum", 0)', 'target': 'patient_id'},
            {'source': 'NULLIF("SheetDefNum", 0)', 'target': 'sheet_def_id'},
            {'source': 'NULLIF("DocNum", 0)', 'target': 'doc_id'},
            {'source': 'NULLIF("ClinicNum", 0)', 'target': 'clinic_id'},
            {'source': 'NULLIF("WebFormSheetID", 0)', 'target': 'web_form_sheet_id'}
        ]) }},
        
        -- Sheet Properties
        "SheetType"::integer as sheet_type,
        "FontSize"::real as font_size,
        nullif(trim("FontName"), '') as font_name,
        "Width"::integer as width,
        "Height"::integer as height,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsLandscape"') }} as is_landscape,
        {{ convert_opendental_boolean('"ShowInTerminal"') }} as show_in_terminal,
        {{ convert_opendental_boolean('"IsWebForm"') }} as is_web_form,
        {{ convert_opendental_boolean('"IsMultiPage"') }} as is_multi_page,
        {{ convert_opendental_boolean('"IsDeleted"') }} as is_deleted,
        {{ convert_opendental_boolean('"HasMobileLayout"') }} as has_mobile_layout,
        
        -- Additional Fields
        nullif(trim("InternalNote"), '') as internal_note,
        nullif(trim("Description"), '') as description,
        "RevID"::integer as revision_id,
        
        -- Date fields using macro
        {{ clean_opendental_date('"DateTimeSheet"') }} as sheet_datetime,
        {{ clean_opendental_date('"DateTSheetEdited"') }} as sheet_edited_datetime,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTimeSheet"',
            updated_at_column='"DateTSheetEdited"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
