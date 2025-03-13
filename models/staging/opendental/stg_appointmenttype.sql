with source as (
    select * from {{ source('opendental', 'appointmenttype') }}
),

renamed as (
    select
        -- Primary key
        AppointmentTypeNum as appointment_type_id,
        
        -- Basic information
        AppointmentTypeName as type_name,
        AppointmentTypeColor as color_value,
        
        -- Display and UI settings
        ItemOrder as sort_order,
        IsHidden as is_hidden,
        
        -- Time pattern (each X represents a 5-minute increment)
        Pattern as time_pattern,
        LENGTH(Pattern) * 5 as duration_minutes,
        
        -- Pattern analysis
        LENGTH(REPLACE(Pattern, '/', '')) as work_block_count,
        LENGTH(REPLACE(Pattern, 'X', '')) as break_block_count,
        CASE
            WHEN Pattern IS NULL THEN 'No Pattern'
            WHEN LENGTH(Pattern) = 0 THEN 'Empty Pattern'
            ELSE 'Valid Pattern'
        END as pattern_status,
        
        -- Associated procedure codes
        CodeStr as associated_procedure_codes,
        CodeStrRequired as required_procedure_codes,
        RequiredProcCodesNeeded as minimum_required_codes_count,
        
        -- Scheduling constraints
        BlockoutTypes as blockout_type_list,
        
        -- Usage status
        CASE
            WHEN IsHidden = 1 THEN 'Hidden'
            WHEN Pattern IS NULL OR LENGTH(Pattern) = 0 THEN 'Invalid Pattern'
            ELSE 'Active'
        END as appointment_type_status
        
    from source
)

select * from renamed
