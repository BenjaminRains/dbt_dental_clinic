with source_data as (
    select * from {{ source('opendental', 'appointmenttype') }}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"AppointmentTypeNum"', 'target': 'appointment_type_id'}
        ]) }},
        
        -- Attributes
        "AppointmentTypeName" as appointment_type_name,
        "AppointmentTypeColor" as appointment_type_color,
        "ItemOrder" as item_order,
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        "Pattern" as pattern,
        "CodeStr" as code_str,
        "CodeStrRequired" as code_str_required,
        "RequiredProcCodesNeeded" as required_proc_codes_needed,
        "BlockoutTypes" as blockout_types,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns

union all

select
    0 as appointment_type_id,
    'None' as appointment_type_name,
    null as appointment_type_color,
    null as item_order,
    null as is_hidden,
    null as pattern,
    null as code_str,
    null as code_str_required,
    null as required_proc_codes_needed,
    null as blockout_types,
    
    -- Metadata columns for default record
    current_timestamp as _loaded_at,
    null as _created_at,
    null as _updated_at,
    null as _created_by_user_id
