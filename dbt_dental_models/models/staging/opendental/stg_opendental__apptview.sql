{{ config(
    materialized='view'
) }}

/*
    Staging model for appointment views.
    Defines different views of the appointment schedule that users can access.
    Each view can have different settings for display preferences, scheduling
    restrictions, and clinic-specific configurations.
*/

with source_data as (
    select * from {{ source('opendental', 'apptview') }}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"ApptViewNum"', 'target': 'appt_view_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'}
        ]) }},
        
        -- Attributes
        "Description" as view_description,
        "ItemOrder" as sort_order,
        "RowsPerIncr" as rows_per_increment,
        "OnlyScheduledProvs" as only_scheduled_providers,
        "OnlySchedBeforeTime" as only_scheduled_before_time,
        "OnlySchedAfterTime" as only_scheduled_after_time,
        "StackBehavUR"::integer as stack_behavior_up_right,
        "StackBehavLR"::integer as stack_behavior_left_right,
        "ApptTimeScrollStart" as appointment_time_scroll_start,
        {{ convert_opendental_boolean('"IsScrollStartDynamic"') }} as is_scroll_start_dynamic,
        {{ convert_opendental_boolean('"IsApptBubblesDisabled"') }} as is_appointment_bubbles_disabled,
        "WidthOpMinimum" as width_operatory_minimum,
        "WaitingRmName" as waiting_room_name,
        "OnlyScheduledProvDays" as only_scheduled_provider_days,
        
        -- Metadata columns
        {{ standardize_metadata_columns() }}

    from source_data
)

select * from renamed_columns
