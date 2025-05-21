{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for appointment views.
    Defines different views of the appointment schedule that users can access.
    Each view can have different settings for display preferences, scheduling
    restrictions, and clinic-specific configurations.
*/

select
    "ApptViewNum" as appt_view_id,
    "Description" as view_description,
    "ItemOrder" as sort_order,
    "RowsPerIncr" as rows_per_increment,
    "OnlyScheduledProvs" as only_scheduled_providers,
    "OnlySchedBeforeTime" as only_scheduled_before_time,
    "OnlySchedAfterTime" as only_scheduled_after_time,
    "StackBehavUR" as stack_behavior_up_right,
    "StackBehavLR" as stack_behavior_left_right,
    "ClinicNum" as clinic_id,
    "ApptTimeScrollStart" as appointment_time_scroll_start,
    "IsScrollStartDynamic" as is_scroll_start_dynamic,
    "IsApptBubblesDisabled" as is_appointment_bubbles_disabled,
    "WidthOpMinimum" as width_operatory_minimum,
    "WaitingRmName" as waiting_room_name,
    "OnlyScheduledProvDays" as only_scheduled_provider_days,
    CURRENT_TIMESTAMP as model_created_at,
    CURRENT_TIMESTAMP as model_updated_at
from {{ source('opendental', 'apptview') }}
