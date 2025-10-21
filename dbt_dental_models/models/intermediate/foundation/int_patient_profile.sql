{{ config(
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
    tags=['foundation', 'weekly']) }}

/*
    Intermediate model for patient profile
    Part of System Foundation: Core Patient Data
    
    This model:
    1. Combines core patient demographics from staging models
    2. Integrates family relationships and emergency contacts
    3. Aggregates patient diseases and document tracking
    4. Provides comprehensive patient profile for downstream analytics
    5. Serves as foundation for all patient-related intermediate models
    
    Business Logic Features:
    - Patient demographics and status categorization
    - Family relationship linkage with aggregated support (arrays for multiple relationships)
    - Emergency contact integration from patient notes
    - Medical and treatment notes consolidation
    - Patient preferences and consent tracking
    - Active disease tracking (count, IDs, statuses)
    - Document management tracking (count, categories)
    
    Data Sources:
    - stg_opendental__patient: Core patient demographics
    - stg_opendental__patientnote: Emergency contacts and medical notes
    - stg_opendental__patientlink: Family relationships
    - stg_opendental__disease: Active disease conditions
    - stg_opendental__document: Patient document tracking
    
    Data Quality Notes:
    - Geographic data (city, state, zipcode) pending investigation of patient-zipcode relationships
    - Consider checking for additional patient address/location tables in source system
    - Family relationships are aggregated to prevent duplicate patient records
    - Disease data filtered to active only (date_stop is null)
    - Document and disease aggregations use LEFT JOIN to handle patients with no records
    
    Performance Considerations:
    - Foundation model materialized as table for downstream performance
    - Indexed on primary relationships for efficient joins
    - Weekly refresh cycle appropriate for demographic data
    - Disease and document aggregations pre-calculated for mart efficiency
*/

-- 1. Source data retrieval with deduplication
with source_patient as (
    select distinct on (patient_id) *
    from {{ ref('stg_opendental__patient') }}
    order by patient_id, _created_at desc
),

-- 2. Lookup/reference data - aggregated to prevent duplicates
patient_family_links as (
    select
        patient_id_from as patient_id,
        -- Aggregate family relationships to prevent duplicates
        array_agg(distinct patient_id_to) as family_ids,
        array_agg(distinct link_type) as family_link_types,
        max(linked_at) as latest_family_link_date,
        count(*) as total_family_links
    from {{ ref('stg_opendental__patientlink') }}
    group by patient_id_from
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

-- 3. Patient disease tracking (active diseases only)
patient_diseases as (
    select 
        patient_id,
        count(*) as disease_count,
        array_agg(disease_def_id::text) as disease_ids,
        array_agg(problem_status::text) as disease_statuses
    from {{ ref('stg_opendental__disease') }}
    where date_stop is null  -- Only active diseases
    group by patient_id
),

-- 4. Patient document tracking
patient_documents as (
    select 
        patient_id,
        count(*) as document_count,
        array_agg(document_category_id::text) as document_categories
    from {{ ref('stg_opendental__document') }}
    group by patient_id
),

-- 5. Business logic transformation
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
            when patient_status = 0 then 'Patient'
            when patient_status = 1 then 'NonPatient'
            when patient_status = 2 then 'Inactive'
            when patient_status = 3 then 'Archived'
            when patient_status = 4 then 'Deceased'
            when patient_status = 5 then 'Deleted'
            else 'Unknown'
        end as patient_status_description,
        
        -- Position code categorization
        case 
            when position_code = 0 then 'Default'
            when position_code = 1 then 'House'
            when position_code = 2 then 'Staff'
            when position_code = 3 then 'VIP'
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
        
        -- Metadata (preserved from source staging model)
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by
        
    from source_patient
),

-- 6. Integration CTE (joins everything together)
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
        
        -- Family relationships (aggregated)
        pfl.family_ids,
        pfl.family_link_types,
        pfl.latest_family_link_date,
        pfl.total_family_links,
        
        -- Emergency contacts and notes
        pnl.ice_name as emergency_contact_name,
        pnl.ice_phone as emergency_contact_phone,
        pnl.medical as medical_notes,
        pnl.treatment as treatment_notes,
        pnl.pronoun,
        pnl.consent,
        pnl.notes_created_at,
        pnl.notes_updated_at,
        
        -- Patient diseases (active diseases only)
        pd.disease_count,
        pd.disease_ids,
        pd.disease_statuses,
        
        -- Patient documents
        doc.document_count,
        doc.document_categories,
        
        -- Metadata fields (standardized pattern)
        {{ standardize_intermediate_metadata(
            primary_source_alias='pde',
            source_metadata_fields=['_loaded_at', '_created_at', '_updated_at', '_created_by']
        ) }}
        
    from patient_demographics_enhanced pde
    left join patient_family_links pfl
        on pde.patient_id = pfl.patient_id
    left join patient_notes_lookup pnl
        on pde.patient_id = pnl.patient_id
    left join patient_diseases pd
        on pde.patient_id = pd.patient_id
    left join patient_documents doc
        on pde.patient_id = doc.patient_id
)

select * from patient_integrated 
