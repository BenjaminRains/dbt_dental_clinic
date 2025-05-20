{{
    config(
        materialized='view'
    )
}}

with user_preferences as (
    select
        user_od_pref_id,
        user_id,
        fkey,
        fkey_type,
        value_string,
        clinic_id
    from {{ ref('stg_opendental__userodpref') }}
),

user_appointment_views as (
    select
        userod_appt_view_id,
        user_id as appt_view_user_id,
        clinic_id as appt_view_clinic_id,
        appt_view_id
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
    
    -- Appointment View Information
    uav.userod_appt_view_id,
    uav.appt_view_id,
    uav.appt_view_clinic_id

from user_preferences up
left join user_appointment_views uav
    on up.user_id = uav.appt_view_user_id
    and up.clinic_id = uav.appt_view_clinic_id 