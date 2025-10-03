{{ config(
        materialized='incremental',
        schema='intermediate',
        unique_key='provider_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        indexes=[
            {'columns': ['provider_id'], 'unique': true},
            {'columns': ['is_active_provider']},
            {'columns': ['_updated_at']}
        ],
        tags=['foundation', 'weekly']) }}

/*
    Intermediate model for provider profile and business logic
    Part of System Foundation: Provider Profile Management
    
    This model:
    1. Transforms provider data into business-friendly attributes and status flags
    2. Applies comprehensive business rules for provider status and classification  
    3. Creates derived fields for provider capabilities and restrictions
    4. Establishes foundation for all provider-related downstream models
    
    Business Logic Features:
    - Provider Status Classification: Active, hidden, terminated, system providers
    - Capability Flags: Can treat patients, prescribe controlled substances, bill procedures
    - Professional Validation: DEA numbers, state licenses, professional IDs
    - UI/Display Properties: Colors, scheduling notes, web descriptions
    - Financial Goals: Hourly production targets
    
    Data Sources:
    - stg_opendental__provider: Primary provider data with professional identifiers
    
    Data Quality Notes:
    - System provider (ID=0) handled as special case
    - Non-person providers (labs, facilities) properly classified
    - Terminated providers identified by termination_date
    - Empty/null professional identifiers handled gracefully
    
    Performance Considerations:
    - Incremental materialization with _updated_at filtering
    - Indexed on provider_id, is_active_provider, and _updated_at
    - Efficient boolean flag calculations using CASE statements
    
    Systems Integration:
    - Foundation for System A: Fee Processing (provider fee schedules)
    - Foundation for System B: Insurance Processing (provider networks)
    - Foundation for System C: Payment Processing (provider billing)
    - Foundation for System G: Scheduling (provider availability)
*/

-- 1. Source data retrieval with incremental filtering
with source_providers as (
    select * from {{ ref('stg_opendental__provider') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

-- 2. Business logic transformation - basic provider flags and classifications
provider_enhanced as (
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
        
        -- Metadata (preserved from source staging model)
        _loaded_at,
        _created_at,
        _updated_at,
        _transformed_at
        
    from source_providers
),

-- 3. Advanced capability flags - derived from basic flags
provider_capabilities as (
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
        
    from provider_enhanced
),

-- 4. Final integration with standardized metadata
provider_integrated as (
    select
        -- Core provider fields
        pc.provider_id,
        pc.provider_abbreviation,
        pc.display_order,
        
        -- Provider name fields
        pc.last_name,
        pc.first_name,
        pc.middle_initial,
        pc.name_suffix,
        pc.preferred_name,
        pc.custom_id,
        
        -- Professional identifiers  
        pc.social_security_number,
        pc.state_license_number,
        pc.dea_number,
        pc.blue_cross_id,
        pc.medicaid_id,
        pc.national_provider_id,
        pc.canadian_office_number,
        pc.ecw_id,
        pc.state_rx_id,
        pc.state_where_licensed,
        pc.taxonomy_code_override,
        
        -- Classification and relationships
        pc.fee_schedule_id,
        pc.specialty_id,
        pc.school_class_id,
        pc.billing_override_provider_id,
        pc.email_address_id,
        
        -- Status and type fields
        pc.provider_status,
        pc.anesthesia_provider_type,
        pc.ehr_mu_stage,
        
        -- UI and display properties
        pc.provider_color,
        pc.outline_color,
        pc.schedule_note,
        pc.web_schedule_description,
        pc.web_schedule_image_location,
        
        -- Financial goals
        pc.hourly_production_goal_amount,
        
        -- Boolean flags
        pc.is_secondary,
        pc.is_hidden,
        pc.is_using_tin,
        pc.has_signature_on_file,
        pc.is_cdanet,
        pc.is_not_person,
        pc.is_instructor,
        pc.is_hidden_report,
        pc.is_erx_enabled,
        
        -- Date fields
        pc.birth_date,
        pc.termination_date,
        
        -- Business logic flags
        pc.is_system_provider,
        pc.is_active_provider,
        pc.is_non_person_provider,
        pc.is_terminated_provider,
        pc.can_prescribe_controlled_substances,
        pc.has_state_license,
        
        -- Capability flags
        pc.can_treat_patients,
        pc.can_bill_procedures,
        
        -- Standardized metadata (provider staging model has _loaded_at, _created_at, _updated_at)
        {{ standardize_intermediate_metadata(primary_source_alias='pc', source_metadata_fields=['_loaded_at', '_created_at', '_updated_at']) }}
        
    from provider_capabilities pc
)

select * from provider_integrated
