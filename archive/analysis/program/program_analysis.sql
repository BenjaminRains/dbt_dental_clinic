with source as (
    select * from {{ source('opendental', 'program') }}
),

renamed as (
    select
        -- Primary key
        ProgramNum as program_id,

        -- Program identification
        ProgName as program_name,
        ProgDesc as program_description,

        -- Status flags
        Enabled as is_enabled_flag,
        IsDisabledByHq as is_disabled_by_hq_flag,

        -- Program configuration
        Path as program_path,
        CommandLine as command_line,
        PluginDllName as plugin_dll_name,

        -- File settings
        FileTemplate as file_template,
        FilePath as file_path,
        ButtonImage as button_image,

        -- Additional information
        Note as program_notes,
        CustErr as custom_error_message
    from source
)

select * from renamed