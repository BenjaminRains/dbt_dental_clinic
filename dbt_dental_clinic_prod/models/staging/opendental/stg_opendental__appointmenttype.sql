{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('opendental', 'appointmenttype') }}
),

renamed_columns as (
    select
        -- Primary Key transformations using macro
        {{ transform_id_columns([
            {'source': '"AppointmentTypeNum"', 'target': 'appointment_type_id'}
        ]) }},
        
        -- Type attributes  
        "AppointmentTypeName" as appointment_type_name,
        "AppointmentTypeColor" as appointment_type_color,
        "ItemOrder" as item_order,
        "Pattern" as pattern,
        "CodeStr" as code_str,
        "CodeStrRequired" as code_str_required,
        "RequiredProcCodesNeeded" as required_proc_codes_needed,
        "BlockoutTypes" as blockout_types,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns() }}

    from source_data
)

select * from renamed_columns