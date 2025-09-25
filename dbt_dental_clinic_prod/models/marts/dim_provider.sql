with Source as (
    select * from {{ ref('stg_opendental__provider') }}
),

Definitions as (
    select
        definition_id,
        category_id,
        item_name,
        item_value,
        item_order,
        item_color
    from {{ ref('stg_opendental__definition') }}
),

ProviderAvailability as (
    select
        provider_id,
        count(distinct schedule_date) as scheduled_days,
        sum(available_minutes) as total_available_minutes,
        avg(available_minutes) as avg_daily_available_minutes,
        count(distinct case when is_day_off then schedule_date end) as days_off_count
    from {{ ref('int_provider_availability') }}
    where schedule_date >= current_date - interval '90 days'
    group by provider_id
),

Enhanced as (
    select
        -- Primary key
        p.provider_id,
        
        -- Provider identifiers
        p.provider_abbreviation,
        p.last_name as provider_last_name,
        p.first_name as provider_first_name,
        p.middle_initial as provider_middle_initial,
        p.name_suffix as provider_suffix,
        p.preferred_name as provider_preferred_name,
        p.custom_id as provider_custom_id,
        
        -- Computed provider name
        case 
            when p.preferred_name is not null and p.preferred_name != '' 
            then p.preferred_name
            else concat(p.first_name, ' ', p.last_name)
        end as provider_name,
        
        -- Provider classifications
        p.fee_schedule_id,
        p.specialty_id,
        def_specialty.item_name as specialty_description,
        p.provider_status,
        def_status.item_name as provider_status_description,
        p.anesthesia_provider_type,
        def_anesth.item_name as anesthesia_provider_type_description,
        
        -- Provider credentials
        p.state_license_number as state_license,
        p.dea_number,
        p.blue_cross_id,
        p.medicaid_id,
        p.national_provider_id,
        p.state_rx_id,
        p.state_where_licensed,
        p.taxonomy_code_override,
        
        -- Provider flags
        p.is_secondary,
        p.is_hidden,
        p.is_using_tin,
        p.has_signature_on_file,
        p.is_cdanet,
        p.is_not_person,
        p.is_instructor,
        p.is_hidden_report,
        p.is_erx_enabled,
        
        -- Provider display properties
        p.provider_color,
        p.outline_color,
        p.schedule_note,
        p.web_schedule_description,
        p.web_schedule_image_location,
        
        -- Financial and goals
        p.hourly_production_goal_amount,
        
        -- Availability metrics
        pa.scheduled_days,
        pa.total_available_minutes,
        pa.avg_daily_available_minutes,
        pa.days_off_count,
        
        -- Calculated availability metrics
        case 
            when pa.scheduled_days > 0 
            then round(pa.total_available_minutes::numeric / pa.scheduled_days, 2)
            else 0 
        end as avg_minutes_per_scheduled_day,
        
        case 
            when pa.scheduled_days > 0 
            then round((pa.scheduled_days - pa.days_off_count)::numeric / pa.scheduled_days * 100, 2)
            else 0 
        end as availability_percentage,
        
        -- Provider status categorization
        case
            when p.termination_date is not null then 'Terminated'
            when p.provider_status = 0 then 'Active'
            when p.provider_status = 1 then 'Inactive'
            else 'Unknown'
        end as provider_status_category,
        
        -- Provider type categorization
        case
            when p.is_instructor then 'Instructor'
            when p.is_secondary then 'Secondary'
            when p.is_not_person then 'Non-Person'
            else 'Primary'
        end as provider_type_category,
        
        -- Metadata
        p._created_at,
        p._updated_at,
        current_timestamp as _loaded_at
    from Source p
    left join ProviderAvailability pa
        on p.provider_id = pa.provider_id
    left join Definitions def_specialty
        on def_specialty.category_id = 3  -- Assuming category_id 3 is for specialties
        and def_specialty.item_value = p.specialty_id::text
    left join Definitions def_status
        on def_status.category_id = 2  -- Assuming category_id 2 is for provider status
        and def_status.item_value = p.provider_status::text
    left join Definitions def_anesth
        on def_anesth.category_id = 7  -- Assuming category_id 7 is for anesthesia provider types
        and def_anesth.item_value = p.anesthesia_provider_type::text
)

select * from Enhanced
