{{
    config(
        materialized='incremental',
        schema='intermediate',
        unique_key='provider_schedule_id',
        on_schema_change='fail',
        incremental_strategy='merge',
        indexes=[
            {'columns': ['provider_schedule_id'], 'unique': true},
            {'columns': ['provider_id']},
            {'columns': ['schedule_date']},
            {'columns': ['provider_id', 'schedule_date']},
            {'columns': ['_updated_at']}
        ]
    )
}}

/*
    Intermediate model for provider availability.
    Part of System G: Scheduling
    
    This model:
    1. Calculates daily schedule windows and availability for each provider
    2. Identifies days off and schedule blocks from provider schedules
    3. Merges overlapping schedule blocks to calculate accurate availability
    4. Provides availability metrics for scheduling and capacity planning
    
    Business Logic Features:
    - Schedule Block Merging: Handles overlapping provider schedule blocks by merging them
    - Availability Calculation: Calculates total available minutes accounting for overlaps
    - Day Off Detection: Identifies when providers have no scheduled time
    - Schedule Status Tracking: Categorizes schedules as single block, multiple blocks, or no schedule
    
    Data Quality Notes:
    - Start and stop times are NULL in source data (expected behavior per staging model documentation)
    - Schedule type 1 represents individual provider schedules
    - Provider schedules may have multiple overlapping blocks that need merging
    
    Performance Considerations:
    - Uses window functions for efficient overlap detection
    - Incremental processing based on schedule_date for performance
    - Indexed on provider_id and schedule_date for fast lookups
*/

-- 1. Source data retrieval
with source_schedule as (
    select 
        schedule_id,
        schedule_date,
        start_time,
        stop_time as end_time,
        provider_id,
        schedule_type,
        status,
        _loaded_at,
        _created_at,
        _updated_at
    from {{ ref('stg_opendental__schedule') }}
    where schedule_type = 1  -- Individual provider schedules
        and provider_id is not null
        and schedule_date >= current_date - interval '{{ var("schedule_window_days", "365") }} days'
        {% if is_incremental() %}
        and _updated_at > (select max(_updated_at) from {{ this }})
        {% endif %}
),

-- 2. Schedule overlap detection and merging
schedule_with_next_start as (
    select
        provider_id,
        schedule_date,
        start_time,
        end_time,
        lead(start_time) over (
            partition by provider_id, schedule_date 
            order by start_time
        ) as next_start_time
    from source_schedule
),

-- 3. Business logic transformation - merge overlapping blocks
merged_schedules as (
    select
        sws.provider_id,
        sws.schedule_date,
        min(sws.start_time) as start_time,
        max(sws.end_time) as end_time,
        -- Calculate total available minutes accounting for overlaps
        sum(
            extract(epoch from (
                case 
                    when sws.next_start_time is null then sws.end_time
                    else least(sws.end_time, sws.next_start_time)
                end - sws.start_time
            ))/60
        ) as available_minutes,
        -- Track schedule status
        case
            when count(*) > 1 then 'Multiple Blocks'
            when count(*) = 1 then 'Single Block'
            else 'No Schedule'
        end as schedule_status,
        -- Preserve metadata from source (use max to get latest)
        max(s._loaded_at) as _loaded_at,
        max(s._created_at) as _created_at,
        max(s._updated_at) as _updated_at
    from schedule_with_next_start sws
    left join source_schedule s 
        on sws.provider_id = s.provider_id 
        and sws.schedule_date = s.schedule_date
    group by sws.provider_id, sws.schedule_date
),

-- 4. Final integration with business logic enhancements
provider_availability_integrated as (
    select
        -- Primary identification using deterministic hash for daily availability
        abs(mod(
            ('x' || substr(md5(
                cast(provider_id as varchar) || '|' ||
                cast(schedule_date as varchar)
            ), 1, 16))::bit(64)::bigint,
            9223372036854775807  -- Max bigint to avoid overflow
        )) as provider_schedule_id,
        
        -- Core business fields
        provider_id,
        schedule_date,
        start_time,
        end_time,
        
        -- Business logic flags
        case 
            when start_time is null or end_time is null then true
            else false
        end as is_day_off,
        
        -- Calculated metrics
        coalesce(available_minutes, 0) as available_minutes,
        schedule_status,
        
        -- Preserve source metadata for data lineage
        _loaded_at,
        _created_at,
        _updated_at
        
    from merged_schedules
)

-- 5. Final output with standardized metadata
select
    provider_schedule_id,
    provider_id,
    schedule_date,
    start_time,
    end_time,
    is_day_off,
    available_minutes,
    schedule_status,
    _loaded_at,
    _created_at,
    _updated_at,
    current_timestamp as _transformed_at
from provider_availability_integrated
