{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('opendental', 'program') }}
),

renamed_columns as (
    select
        -- ID Columns (with safe conversion)
        {{ transform_id_columns([
            {'source': '"ProgramNum"', 'target': 'program_id'}
        ]) }},
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"Enabled"') }} as is_enabled,
        {{ convert_opendental_boolean('"IsDisabledByHq"') }} as is_disabled_by_hq,
        
        -- Program Configuration
        "ProgName" as program_name,
        "ProgDesc" as program_description,
        "Path" as program_path,
        "CommandLine" as command_line,
        "PluginDllName" as plugin_dll_name,
        "ButtonImage" as button_image,
        "FileTemplate" as file_template,
        "FilePath" as file_path,
        "Note" as note,
        "CustErr" as custom_error,

        -- Standardized metadata columns
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
