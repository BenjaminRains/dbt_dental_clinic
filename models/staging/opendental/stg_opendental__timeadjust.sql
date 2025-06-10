{{ config(
    materialized='incremental',
    unique_key='time_adjust_id',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'timeadjust') }}
    where "TimeEntry" >= '2023-01-01'
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TimeAdjustNum"', 'target': 'time_adjust_id'},
            {'source': 'NULLIF("EmployeeNum", 0)', 'target': 'employee_id'},
            {'source': 'NULLIF("ClinicNum", 0)', 'target': 'clinic_id'},
            {'source': 'NULLIF("PtoDefNum", 0)', 'target': 'pto_def_id'},
            {'source': 'NULLIF("SecuUserNumEntry", 0)', 'target': 'secu_user_entry_id'}
        ]) }},
        
        -- Timestamps
        "TimeEntry" as time_entry_ts,
        
        -- Time durations
        "RegHours" as regular_hours,
        "OTimeHours" as overtime_hours,
        "PtoHours" as pto_hours,
        
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
