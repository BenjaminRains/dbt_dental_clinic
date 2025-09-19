{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='user_od_pref_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['user_od_pref_id'], 'unique': true},
            {'columns': ['user_id']},
            {'columns': ['clinic_id']}
        ]
    )
}}

/*
    Intermediate model for user preferences and appointment views.
    Part of System G: Scheduling
    
    This model:
    1. Consolidates user-specific preferences and settings
    2. Links user appointment view configurations with detailed view settings
    3. Maintains user-clinic specific scheduling preferences
    4. Supports appointment scheduling and task management workflows
    5. Integrates with System G scheduling models for comprehensive user context
    
    Business Logic Features:
    - User Preference Management: Consolidates all user-specific settings from userodpref
    - Appointment View Integration: Links user appointment views with detailed configuration
    - Task View Preferences: Captures task management view settings (fkey_type = 3)
    - Clinic-Specific Configuration: Maintains user preferences per clinic context
    
    Data Quality Notes:
    - User preferences may have multiple entries per user-clinic combination
    - Appointment views are optional and may not exist for all users
    - Task view preferences are filtered to fkey_type = 3 only
    
    Performance Considerations:
    - Table materialization for complex joins across multiple preference tables
    - Indexed on user_id and clinic_id for efficient downstream joins
*/

-- 1. Source data retrieval
with source_user_preferences as (
    select
        user_od_pref_id,
        user_id,
        fkey,
        fkey_type,
        value_string,
        clinic_id,
        _loaded_at,
        _transformed_at
    from {{ ref('stg_opendental__userodpref') }}
),

source_user_appointment_views as (
    select
        userod_appt_view_id,
        user_id as appt_view_user_id,
        clinic_id as appt_view_clinic_id,
        appt_view_id,
        _loaded_at as view_loaded_at,
        _transformed_at as view_transformed_at
    from {{ ref('stg_opendental__userodapptview') }}
),

source_appointment_views as (
    select
        appt_view_id,
        view_description,
        sort_order,
        rows_per_increment,
        only_scheduled_providers,
        only_scheduled_before_time,
        only_scheduled_after_time,
        stack_behavior_up_right,
        stack_behavior_left_right,
        clinic_id as view_clinic_id,
        appointment_time_scroll_start,
        is_scroll_start_dynamic,
        is_appointment_bubbles_disabled,
        width_operatory_minimum,
        waiting_room_name,
        only_scheduled_provider_days,
        _loaded_at as av_loaded_at,
        _transformed_at as av_transformed_at
    from {{ ref('stg_opendental__apptview') }}
),

-- 2. Lookup/reference data
task_view_preferences as (
    select
        user_id,
        clinic_id,
        value_string as task_view_settings,
        _transformed_at as task_view_updated_at
    from {{ ref('stg_opendental__userodpref') }}
    where fkey_type = 3
),

-- 3. Business logic transformation
user_preferences_enhanced as (
    select
        -- Primary identification
        up.user_od_pref_id,
        up.user_id,
        up.clinic_id,
        
        -- User preference details
        up.fkey,
        up.fkey_type,
        up.value_string,
        
        -- Appointment view information
        uav.userod_appt_view_id,
        uav.appt_view_id,
        uav.appt_view_clinic_id,
        
        -- Appointment view details
        av.view_description,
        av.sort_order,
        av.rows_per_increment,
        av.only_scheduled_providers,
        av.only_scheduled_before_time,
        av.only_scheduled_after_time,
        av.stack_behavior_up_right,
        av.stack_behavior_left_right,
        av.view_clinic_id,
        av.appointment_time_scroll_start,
        av.is_scroll_start_dynamic,
        av.is_appointment_bubbles_disabled,
        av.width_operatory_minimum,
        av.waiting_room_name,
        av.only_scheduled_provider_days,
        
        -- Task view preferences
        tvp.task_view_settings,
        tvp.task_view_updated_at,
        
        -- Business logic flags
        case 
            when uav.userod_appt_view_id is not null then true 
            else false 
        end as has_appointment_view,
        
        case 
            when tvp.task_view_settings is not null then true 
            else false 
        end as has_task_view_preferences,
        
        case 
            when up.fkey_type = 3 then true 
            else false 
        end as is_task_view_preference,
        
        -- Metadata (manually implemented for clarity and reliability)
        up._loaded_at,
        up._transformed_at as _source_transformed_at,
        current_timestamp as _transformed_at
        
    from source_user_preferences up
    left join source_user_appointment_views uav
        on up.user_id = uav.appt_view_user_id
        and up.clinic_id = uav.appt_view_clinic_id
    left join source_appointment_views av
        on uav.appt_view_id = av.appt_view_id
    left join task_view_preferences tvp
        on up.user_id = tvp.user_id
        and up.clinic_id = tvp.clinic_id
)

select * from user_preferences_enhanced 
