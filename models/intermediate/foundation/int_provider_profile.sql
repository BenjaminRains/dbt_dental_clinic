{{ config(
    materialized='incremental',
    unique_key='provider_id',
    schema='intermediate'
) }}

/*
    Intermediate model for provider profile and business logic.
    Transforms provider data into business-friendly attributes and status flags.
    
    This model:
    1. Gets provider data from stg_opendental__provider
    2. Applies business rules for provider status and classification
    3. Creates derived fields for provider capabilities and restrictions
*/

with provider_base as (
    select * from {{ ref('stg_opendental__provider') }}
    {% if is_incremental() %}
    where _updated_at > (select max(_updated_at) from {{ this }})
    {% endif %}
),

provider_with_basic_flags as (
    select
        -- Core identifiers
        provider_id,
        provider_abbreviation,
        display_order,
        
        -- Provider name fields
        last_name,
        first_name,
        middle_initial,
        name_suffix,
        preferred_name,
        custom_id,
        
        -- Professional identifiers  
        social_security_number,
        state_license_number,
        dea_number,
        blue_cross_id,
        medicaid_id,
        national_provider_id,
        canadian_office_number,
        ecw_id,
        state_rx_id,
        state_where_licensed,
        taxonomy_code_override,
        
        -- Classification and relationships
        fee_schedule_id,
        specialty_id,
        school_class_id,
        billing_override_provider_id,
        email_address_id,
        
        -- Status and type fields
        provider_status,
        anesthesia_provider_type,
        ehr_mu_stage,
        
        -- UI and display properties
        provider_color,
        outline_color,
        schedule_note,
        web_schedule_description,
        web_schedule_image_location,
        
        -- Financial goals
        hourly_production_goal_amount,
        
        -- Direct boolean fields from staging
        is_secondary,
        is_hidden,
        is_using_tin,
        has_signature_on_file,
        is_cdanet,
        is_not_person,
        is_instructor,
        is_hidden_report,
        is_erx_enabled,
        
        -- Date fields
        birth_date,
        termination_date,
        
        -- Business logic flags (moved from staging)
        case 
            when provider_id = 0 then true
            else false
        end as is_system_provider,
        
        case
            when is_hidden = false and provider_status = 0 then true
            else false
        end as is_active_provider,
        
        -- Additional business logic
        case
            when is_not_person = true then true
            when last_name is null or trim(last_name) = '' then true
            else false
        end as is_non_person_provider,
        
        case
            when termination_date is not null then true
            else false
        end as is_terminated_provider,
        
        case
            when dea_number is not null and trim(dea_number) != '' then true
            else false
        end as can_prescribe_controlled_substances,
        
        case
            when state_license_number is not null and trim(state_license_number) != '' then true
            else false
        end as has_state_license,
        
        -- Metadata
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by_user_id
        
    from provider_base
),

provider_profile as (
    select
        *,
        -- Provider capability flags (using previously defined flags)
        case
            when is_active_provider = true 
                and is_non_person_provider = false 
                and is_terminated_provider = false then true
            else false
        end as can_treat_patients,
        
        case
            when billing_override_provider_id is not null then false
            when is_active_provider = true then true
            else false
        end as can_bill_procedures
        
    from provider_with_basic_flags
)

select * from provider_profile