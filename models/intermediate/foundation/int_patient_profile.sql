{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='patient_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['patient_id'], 'unique': true},
            {'columns': ['guarantor_id']},
            {'columns': ['primary_provider_id']},
            {'columns': ['_updated_at']}
        ],
        tags=['foundation', 'weekly']
    )
}}

/*
    Intermediate model for patient profile
    Part of System Foundation: Core Patient Data
    
    This model:
    1. Combines core patient demographics from staging models
    2. Integrates family relationships and emergency contacts
    3. Provides comprehensive patient profile for downstream analytics
    4. Serves as foundation for all patient-related intermediate models
    
    Business Logic Features:
    - Patient demographics and status categorization
    - Family relationship linkage with bidirectional support
    - Emergency contact integration from patient notes
    - Medical and treatment notes consolidation
    - Patient preferences and consent tracking
    
    Data Quality Notes:
    - Geographic data (city, state, zipcode) pending investigation of patient-zipcode relationships
    - Consider checking for additional patient address/location tables in source system
    - Family relationships may have multiple entries per patient
    
    Performance Considerations:
    - Foundation model materialized as table for downstream performance
    - Indexed on primary relationships for efficient joins
    - Weekly refresh cycle appropriate for demographic data
*/

-- 1. Source data retrieval
with source_patient as (
    select * from {{ ref('stg_opendental__patient') }}
),

-- 2. Lookup/reference data
patient_family_links as (
    select
        patient_id_from as patient_id,
        patient_id_to as family_id,
        link_type,
        linked_at
    from {{ ref('stg_opendental__patientlink') }}
),

patient_notes_lookup as (
    select
        patient_id,
        ice_name,
        ice_phone,
        medical,
        treatment,
        pronoun,
        consent,
        _created_at as notes_created_at,
        _updated_at as notes_updated_at
    from {{ ref('stg_opendental__patientnote') }}
),

-- 3. Business logic transformation
patient_demographics_enhanced as (
    select
        -- Primary identification
        patient_id,
        guarantor_id,
        primary_provider_id,
        secondary_provider_id,
        clinic_id,
        fee_schedule_id,
        
        -- Core demographics
        preferred_name,
        middle_initial,
        gender,
        language,
        birth_date,
        age,
        
        -- Status and categorization
        patient_status,
        position_code,
        student_status,
        
        -- Patient status categorization
        case 
            when patient_status = 0 then 'Active'
            when patient_status = 1 then 'Archived'
            when patient_status = 2 then 'Deceased'
            when patient_status = 3 then 'Prospective'
            else 'Unknown'
        end as patient_status_description,
        
        -- Position code categorization
        case 
            when position_code = 0 then 'Regular Patient'
            when position_code = 1 then 'House Account'
            when position_code = 2 then 'Staff Member'
            when position_code = 3 then 'VIP Patient'
            when position_code = 4 then 'Other'
            else 'Unknown'
        end as patient_category,
        
        -- Contact preferences
        preferred_confirmation_method,
        preferred_contact_method,
        text_messaging_consent,
        
        -- Financial information
        estimated_balance,
        total_balance,
        has_insurance_flag,
        
        -- Important dates
        first_visit_date,
        
        -- Metadata
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by_user_id
    from source_patient
),

-- 4. Integration CTE (joins everything together)
patient_integrated as (
    select
        -- Core patient fields
        pde.patient_id,
        pde.guarantor_id,
        pde.primary_provider_id,
        pde.secondary_provider_id,
        pde.clinic_id,
        pde.fee_schedule_id,
        
        -- Demographics
        pde.preferred_name,
        pde.middle_initial,
        pde.gender,
        pde.language,
        pde.birth_date,
        pde.age,
        
        -- Status and categorization
        pde.patient_status,
        pde.patient_status_description,
        pde.position_code,
        pde.patient_category,
        pde.student_status,
        
        -- Contact preferences
        pde.preferred_confirmation_method,
        pde.preferred_contact_method,
        pde.text_messaging_consent,
        
        -- Financial information
        pde.estimated_balance,
        pde.total_balance,
        pde.has_insurance_flag,
        
        -- Important dates
        pde.first_visit_date,
        
        -- Family relationships
        pfl.family_id,
        pfl.link_type as family_link_type,
        pfl.linked_at as family_linked_at,
        
        -- Emergency contacts and notes
        pnl.ice_name as emergency_contact_name,
        pnl.ice_phone as emergency_contact_phone,
        pnl.medical as medical_notes,
        pnl.treatment as treatment_notes,
        pnl.pronoun,
        pnl.consent,
        pnl.notes_created_at,
        pnl.notes_updated_at,
        
        -- Metadata fields (standardized pattern)
        pde._loaded_at,
        pde._created_at,
        pde._updated_at,
        pde._created_by_user_id,
        current_timestamp as _transformed_at
        
    from patient_demographics_enhanced pde
    left join patient_family_links pfl
        on pde.patient_id = pfl.patient_id
    left join patient_notes_lookup pnl
        on pde.patient_id = pnl.patient_id
)

select * from patient_integrated 