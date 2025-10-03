{{ config(
    materialized='incremental',
    unique_key='time_adjust_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'timeadjust') }}
    where {{ clean_opendental_date('"TimeEntry"') }} >= '2023-01-01'
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TimeAdjustNum"', 'target': 'time_adjust_id'},
            {'source': '"EmployeeNum"', 'target': 'employee_id'},
            {'source': 'CASE WHEN "ClinicNum" = 0 THEN 0 ELSE NULLIF("ClinicNum", 0) END', 'target': 'clinic_id'},
            {'source': 'CASE WHEN "PtoDefNum" = 0 THEN 0 ELSE NULLIF("PtoDefNum", 0) END', 'target': 'pto_def_id'},
            {'source': 'CASE WHEN "SecuUserNumEntry" = 0 THEN 0 ELSE NULLIF("SecuUserNumEntry", 0) END', 'target': 'secu_user_entry_id'}
        ]) }},
        
        -- Timestamps
        {{ clean_opendental_date('"TimeEntry"') }} as time_entry_ts,
        
        -- Time durations - convert time values to hours
        EXTRACT(EPOCH FROM "RegHours")/3600 as regular_hours,
        EXTRACT(EPOCH FROM "OTimeHours")/3600 as overtime_hours,
        EXTRACT(EPOCH FROM "PtoHours")/3600 as pto_hours,
        
        -- Flags and attributes using boolean conversion macro
        {{ convert_opendental_boolean('"IsAuto"') }} as is_auto,
        {{ convert_opendental_boolean('"IsUnpaidProtectedLeave"') }} as is_unpaid_protected_leave,
        "Note" as note,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"TimeEntry"',
            updated_at_column='"TimeEntry"',
            created_by_column='"SecuUserNumEntry"'
        ) }}
        
    from source_data
)

select * from renamed_columns
