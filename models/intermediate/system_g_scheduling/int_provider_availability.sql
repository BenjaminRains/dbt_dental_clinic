{{ config(
    materialized='incremental',
    
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

with provider_schedules as (
    SELECT
        schedule_id,
        schedule_date,
        start_time,
        stop_time as end_time,
        provider_id,
        schedule_type,
        status,
        _created_at
    FROM {{ ref('stg_opendental__schedule') }}
    WHERE schedule_type = 1  -- Individual provider schedules
        AND provider_id IS NOT NULL
        AND schedule_date >= CURRENT_DATE - INTERVAL '{{ var("schedule_window_days", "365") }} days'
),

-- Handle overlapping schedule blocks by merging them
merged_schedules as (
    with schedule_with_next_start as (
        SELECT
            provider_id,
            schedule_date,
            start_time,
            end_time,
            LEAD(start_time) OVER (PARTITION BY provider_id, schedule_date ORDER BY start_time) as next_start_time
        from provider_schedules
    )
    SELECT
        provider_id,
        schedule_date,
        MIN(start_time) as start_time,
        MAX(end_time) as end_time,
        -- Calculate total available minutes accounting for overlaps
        SUM(
            EXTRACT(EPOCH FROM (
                CASE 
                    WHEN next_start_time IS NULL THEN end_time
                    ELSE LEAST(end_time, next_start_time)
                END - start_time
            ))/60
        ) as available_minutes,
        -- Track schedule status
        CASE
            WHEN COUNT(*) > 1 THEN 'Multiple Blocks'
            WHEN COUNT(*) = 1 THEN 'Single Block'
            ELSE 'No Schedule'
        END as schedule_status
    from schedule_with_next_start
    GROUP BY provider_id, schedule_date
),

daily_availability as (
    SELECT
        md5(COALESCE(provider_id::text, '') || COALESCE(schedule_date::text, '')) as provider_schedule_id,
        provider_id,
        schedule_date,
        start_time,
        end_time,
        CASE 
            WHEN start_time IS NULL OR end_time IS NULL THEN true
            ELSE false
        END as is_day_off,
        COALESCE(available_minutes, 0) as available_minutes,
        schedule_status
    from merged_schedules
)

SELECT
    provider_schedule_id,
    provider_id,
    schedule_date,
    start_time,
    end_time,
    is_day_off,
    available_minutes,
    schedule_status,
    CURRENT_TIMESTAMP as model_created_at,
    CURRENT_TIMESTAMP as model_updated_at
from daily_availability

{% if is_incremental() %}
WHERE schedule_date >= (SELECT MAX(schedule_date) FROM {{ this }})
{% endif %}
