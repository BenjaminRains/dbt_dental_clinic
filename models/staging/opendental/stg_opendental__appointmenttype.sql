with source as (
    select * from {{ source('opendental', 'appointmenttype') }}
),

renamed as (
    select
        -- primary key
        "AppointmentTypeNum" as appointment_type_id,
        
        -- attributes
        "AppointmentTypeName" as appointment_type_name,
        "AppointmentTypeColor" as appointment_type_color,
        "ItemOrder" as item_order,
        "IsHidden" as is_hidden,
        "Pattern" as pattern,
        "CodeStr" as code_str,
        "CodeStrRequired" as code_str_required,
        "RequiredProcCodesNeeded" as required_proc_codes_needed,
        "BlockoutTypes" as blockout_types
    from source
)

select * from renamed

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
    null as blockout_types
