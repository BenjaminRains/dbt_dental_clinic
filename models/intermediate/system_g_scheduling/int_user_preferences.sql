{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='user_od_pref_id'
    )
}}

/*
    Intermediate model for user preferences and appointment views.
    Tracks user-specific settings and appointment view configurations.
    
    This model:
    1. Joins user preferences with appointment view settings
    2. Maintains user-clinic specific configurations
    3. Supports appointment scheduling preferences
    4. Integrates with System G scheduling models
    5. Links with appointment and task views
*/

with user_preferences as (
    select
        user_od_pref_id,
        user_id,
        fkey,
        fkey_type,
        value_string,
        clinic_id,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at
    from {{ ref('stg_opendental__userodpref') }}
),

user_appointment_views as (
    select
        userod_appt_view_id,
        user_id as appt_view_user_id,
        clinic_id as appt_view_clinic_id,
        appt_view_id,
        CURRENT_TIMESTAMP as view_created_at,
        CURRENT_TIMESTAMP as view_updated_at
    from {{ ref('stg_opendental__userodapptview') }}
),

-- Add appointment view details
appointment_view_details as (
    select
        av.appt_view_id,
        av.view_description,
        av.sort_order,
        av.rows_per_increment,
        av.only_scheduled_providers,
        av.only_scheduled_before_time,
        av.only_scheduled_after_time,
        av.stack_behavior_up_right,
        av.stack_behavior_left_right,
        av.clinic_id,
        av.appointment_time_scroll_start,
        av.is_scroll_start_dynamic,
        av.is_appointment_bubbles_disabled,
        av.width_operatory_minimum,
        av.waiting_room_name,
        av.only_scheduled_provider_days
    from {{ ref('stg_opendental__apptview') }} av
),

-- Add task view preferences
task_view_preferences as (
    select
        user_id,
        clinic_id,
        value_string as task_view_settings,
        CURRENT_TIMESTAMP as task_view_updated_at
    from {{ ref('stg_opendental__userodpref') }}
    where fkey_type = 'TaskView'
)

select
    -- User Preferences
    up.user_od_pref_id,
    up.user_id,
    up.fkey,
    up.fkey_type,
    up.value_string,
    up.clinic_id,
    up.created_at,
    up.updated_at,
    
    -- Appointment View Information
    uav.userod_appt_view_id,
    uav.appt_view_id,
    uav.appt_view_clinic_id,
    uav.view_created_at,
    uav.view_updated_at,
    
    -- Appointment View Details
    avd.view_description,
    avd.sort_order,
    avd.rows_per_increment,
    avd.only_scheduled_providers,
    avd.only_scheduled_before_time,
    avd.only_scheduled_after_time,
    avd.stack_behavior_up_right,
    avd.stack_behavior_left_right,
    avd.clinic_id as view_clinic_id,
    avd.appointment_time_scroll_start,
    avd.is_scroll_start_dynamic,
    avd.is_appointment_bubbles_disabled,
    avd.width_operatory_minimum,
    avd.waiting_room_name,
    avd.only_scheduled_provider_days,
    
    -- Task View Preferences
    tvp.task_view_settings,
    tvp.task_view_updated_at,
    
    -- Metadata
    CURRENT_TIMESTAMP as model_created_at,
    CURRENT_TIMESTAMP as model_updated_at

from user_preferences up
left join user_appointment_views uav
    on up.user_id = uav.appt_view_user_id
    and up.clinic_id = uav.appt_view_clinic_id
left join appointment_view_details avd
    on uav.appt_view_id = avd.appt_view_id
left join task_view_preferences tvp
    on up.user_id = tvp.user_id
    and up.clinic_id = tvp.clinic_id 