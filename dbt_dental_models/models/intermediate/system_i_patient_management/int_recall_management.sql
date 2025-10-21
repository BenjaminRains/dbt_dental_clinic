{{
    config(
        materialized='incremental',
        unique_key='recall_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['patient_id'], 'type': 'btree'},
            {'columns': ['recall_status_code'], 'type': 'btree'},
            {'columns': ['date_due'], 'type': 'btree'},
            {'columns': ['is_overdue'], 'type': 'btree'}
        ]
    )
}}

-- Intermediate model for recall management data
-- Integrates recall, recalltype, and recalltrigger tables with business logic
-- Replaces direct staging references in mart_hygiene_retention.sql

with 

-- 1. Source data from staging
source_recall as (
    select * from {{ ref('stg_opendental__recall') }}
),

source_recalltype as (
    select * from {{ ref('stg_opendental__recalltype') }}
),

source_recalltrigger as (
    select * from {{ ref('stg_opendental__recalltrigger') }}
),

-- 2. Recall type processing with corrupted default_interval handling
recall_type_enhanced as (
    select
        recall_type_id,
        description as recall_type_description,
        procedures as recall_procedures,
        
        -- Handle corrupted default_interval values
        case 
            when default_interval = 0 then 6  -- Default to 6 months
            when default_interval = 196609 then 6  -- Map corrupted value to 6 months
            when default_interval = 393216 then 12  -- Map corrupted value to 12 months
            when default_interval = 393217 then 12  -- Map corrupted value to 12 months
            when default_interval = 16777217 then 18  -- Map corrupted value to 18 months
            when default_interval = 83886081 then 24  -- Map corrupted value to 24 months
            else 6  -- Default fallback
        end as recall_interval_months,
        
        -- Flag corrupted intervals for data quality tracking
        case 
            when default_interval in (0, 196609, 393216, 393217, 16777217, 83886081) 
            then true 
            else false 
        end as has_corrupted_interval,
        
        -- Use time_pattern as backup if available
        time_pattern,
        
        -- Metadata
        _loaded_at
        
    from source_recalltype
),

-- 3. Recall trigger processing
recall_trigger_enhanced as (
    select
        recall_type_id,
        array_agg(code_id order by code_id) as trigger_procedure_code_ids,
        count(*) as trigger_count
    from source_recalltrigger
    group by recall_type_id
),

-- 4. Main recall processing with business logic
recall_enhanced as (
    select
        -- Primary keys
        r.recall_id,
        r.patient_id,
        r.recall_type_id,
        
        -- Recall attributes
        rt.recall_type_description,
        rt.recall_interval_months,
        rt.recall_procedures,
        rt.has_corrupted_interval,
        rt.time_pattern,
        
        -- Trigger information
        rtt.trigger_procedure_code_ids,
        rtt.trigger_count,
        
        -- Status and timing
        r.recall_status as recall_status_code,
        case r.recall_status
            when 0 then 'Active'           -- Patient needs to be scheduled (5,188 records)
            when 95 then 'Scheduled'      -- Appointment already booked (1 record)
            when 298 then 'Completed'     -- Patient completed recall visit (5 records)
            else 'Unknown'                -- Handle any unexpected values
        end as recall_status_description,
        
        r.date_due,
        r.date_previous,
        r.date_scheduled,
        r.disable_until_date,
        
        -- Calculated metrics
        case 
            when r.date_due is not null 
            then current_date - r.date_due 
            else null 
        end as days_overdue,
        
        case 
            when r.date_previous is not null and r.date_due is not null
            then r.date_due - r.date_previous
            else null 
        end as days_since_previous,
        
        -- Boolean flags
        case 
            when r.date_due is not null and current_date > r.date_due 
            then true 
            else false 
        end as is_overdue,
        
        case 
            when r.recall_status = 95 
            then true 
            else false 
        end as is_scheduled,
        
        case 
            when r.disable_until_date is not null and current_date < r.disable_until_date
            then true 
            else false 
        end as is_disabled,
        
        -- Compliance status
        case 
            when r.recall_status = 298 then 'Compliant'
            when r.recall_status = 95 and r.date_scheduled <= r.date_due then 'Compliant'
            when r.recall_status = 0 and r.date_due is not null and current_date > r.date_due then 'Overdue'
            when r.recall_status = 0 and r.date_due is not null and current_date <= r.date_due then 'Due Soon'
            when r.recall_status = 0 and r.date_due is null then 'No Due Date'
            else 'Unknown'
        end as compliance_status,
        
        -- Priority score (0-100)
        case 
            when r.recall_status = 0 and r.date_due is not null and current_date > r.date_due + interval '90 days' then 100
            when r.recall_status = 0 and r.date_due is not null and current_date > r.date_due + interval '60 days' then 80
            when r.recall_status = 0 and r.date_due is not null and current_date > r.date_due + interval '30 days' then 60
            when r.recall_status = 0 and r.date_due is not null and current_date > r.date_due then 40
            when r.recall_status = 0 and r.date_due is not null and current_date > r.date_due - interval '30 days' then 20
            when r.recall_status = 0 and r.date_due is not null then 10
            else 0
        end as priority_score,
        
        -- Data quality flags
        case 
            when r.recall_id is not null and r.patient_id is not null and r.recall_type_id is not null
            then true 
            else false 
        end as is_valid_recall,
        
        case 
            when r.date_due is not null and r.date_due > current_date + interval '1 year'
            then true 
            else false 
        end as is_future_dated,
        
        case 
            when r.date_scheduled is not null 
            then true 
            else false 
        end as has_scheduled_appointment
        
    from source_recall r
    left join recall_type_enhanced rt
        on r.recall_type_id = rt.recall_type_id
    left join recall_trigger_enhanced rtt
        on r.recall_type_id = rtt.recall_type_id
),

-- 5. Final validation and filtering with standardized metadata
final as (
    select 
        re.*,
        
        -- Standardized intermediate metadata
        {{ standardize_intermediate_metadata(
            primary_source_alias='sr',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']
        ) }}
        
    from recall_enhanced re
    inner join source_recall sr
        on re.recall_id = sr.recall_id
    where re.is_valid_recall = true  -- Only include valid recalls
)

select * from final

{% if is_incremental() %}
    -- Incremental logic: only process new or updated records
    where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
