{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'program') }}
),

renamed as (
    select
        -- Primary Key
        "ProgramNum" as program_id,
        
        -- Attributes
        "ProgName" as program_name,
        "ProgDesc" as program_description,
        "Enabled" as is_enabled,
        "Path" as program_path,
        "CommandLine" as command_line,
        "Note" as note,
        "PluginDllName" as plugin_dll_name,
        "ButtonImage" as button_image,
        "FileTemplate" as file_template,
        "FilePath" as file_path,
        "IsDisabledByHq" as is_disabled_by_hq,
        "CustErr" as custom_error,

        -- Metadata
        current_timestamp as _loaded_at

    from source
)

select * from renamed
