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
    
    Part of System G: Scheduling
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
    
    -- Metadata
    CURRENT_TIMESTAMP as model_created_at,
    CURRENT_TIMESTAMP as model_updated_at

from user_preferences up
left join user_appointment_views uav
    on up.user_id = uav.appt_view_user_id
    and up.clinic_id = uav.appt_view_clinic_id 