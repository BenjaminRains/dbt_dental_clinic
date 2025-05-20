{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key='provider_schedule_id'
) }}

/*
    Intermediate model for provider availability.
    Calculates daily schedule windows and availability for each provider.
    
    This model:
    1. Gets provider schedules from stg_opendental__schedule
    2. Identifies days off and schedule blocks
    3. Calculates daily availability windows
*/

WITH ProviderSchedules AS (
    SELECT
        schedule_id,
        schedule_date,
        start_time,
        stop_time as end_time,
        provider_id,
        schedule_type,
        status,
        created_at
    FROM {{ ref('stg_opendental__schedule') }}
    WHERE schedule_type = 0  -- Provider schedules
        AND provider_id IS NOT NULL
        AND schedule_date >= CURRENT_DATE - INTERVAL '90 days'
),

DailyAvailability AS (
    SELECT
        md5(COALESCE(provider_id::text, '') || COALESCE(schedule_date::text, '')) as provider_schedule_id,
        provider_id,
        schedule_date,
        MIN(start_time) as start_time,
        MAX(end_time) as end_time,
        CASE 
            WHEN MIN(start_time) IS NULL OR MAX(end_time) IS NULL THEN true
            ELSE false
        END as is_day_off,
        EXTRACT(EPOCH FROM (MAX(end_time) - MIN(start_time)))/60 as available_minutes
    FROM ProviderSchedules
    GROUP BY provider_id, schedule_date
)

SELECT
    provider_schedule_id,
    provider_id,
    schedule_date,
    start_time,
    end_time,
    is_day_off,
    available_minutes,
    CURRENT_TIMESTAMP as model_created_at,
    CURRENT_TIMESTAMP as model_updated_at
FROM DailyAvailability

{% if is_incremental() %}
WHERE schedule_date >= (SELECT MAX(schedule_date) FROM {{ this }})
{% endif %}
