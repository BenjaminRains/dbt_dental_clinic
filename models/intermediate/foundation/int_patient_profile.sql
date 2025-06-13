{{
    config(
        materialized='table',
        schema='intermediate',
        unique_key='patient_id',
        tags=['foundation', 'weekly'],
        description='''
            # int_patient_profile

            Foundation model combining core patient information from staging models to 
            create comprehensive patient profiles.

            ## Key Features
            - Basic demographics and contact information
            - Family relationships
            - Emergency contacts
            - Medical notes and treatment information
            - Patient preferences (pronouns, consent)

            ## Usage
            Primary source for patient profile information in downstream analytics.

            ## Notes
            - Geographic data (city, state, zipcode) pending investigation of patient-zipcode
             relationships
            - Consider checking for additional patient address/location tables in source system
        '''
    )
}}

with source_data as (
    select * from {{ ref('stg_opendental__patient') }}
),

-- Standardize IDs and metadata
standardized as (
    select
        -- Core patient information
        patient_id,
        guarantor_id,
        primary_provider_id,
        secondary_provider_id,
        clinic_id,
        fee_schedule_id,
        middle_initial,
        preferred_name,
        gender,
        language,
        birth_date,
        age,
        patient_status,
        position_code,
        student_status,
        preferred_confirmation_method,
        preferred_contact_method,
        text_messaging_consent,
        estimated_balance,
        total_balance,
        has_insurance_flag,
        first_visit_date,
        -- Metadata columns
        _loaded_at,
        _created_at,
        _updated_at,
        _created_by_user_id
    from source_data
),

-- Family relationships
family_relationships as (
    select
        patient_id_from as patient_id,
        patient_id_to as family_id,
        link_type,
        linked_at
    from {{ ref('stg_opendental__patientlink') }}
),

-- Patient notes and additional information
patient_notes as (
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

-- Combine all patient information
final as (
    select
        -- Required metadata fields
        s._loaded_at,
        s._created_at,
        s._updated_at,
        s._created_by_user_id,
        current_timestamp as _transformed_at,
        
        -- Core patient information
        s.patient_id,
        s.guarantor_id,
        s.primary_provider_id,
        s.secondary_provider_id,
        s.clinic_id,
        s.fee_schedule_id,
        s.preferred_name,
        s.middle_initial,
        s.gender,
        s.language,
        s.birth_date,
        s.age,
        s.patient_status,
        s.position_code,
        s.student_status,
        s.preferred_confirmation_method,
        s.preferred_contact_method,
        s.text_messaging_consent,
        s.estimated_balance,
        s.total_balance,
        s.has_insurance_flag,
        
        -- Family relationships
        f.family_id,
        f.link_type as family_link_type,
        f.linked_at as family_linked_at,
        
        -- Emergency contacts and notes
        pn.ice_name as emergency_contact_name,
        pn.ice_phone as emergency_contact_phone,
        pn.medical as medical_notes,
        pn.treatment as treatment_notes,
        pn.pronoun,
        pn.consent,
        
        -- Dates
        s.first_visit_date,
        s._created_at as patient_created_at,
        s._updated_at as patient_updated_at,
        pn.notes_created_at,
        pn.notes_updated_at
    from standardized s
    left join family_relationships f
        on s.patient_id = f.patient_id
    left join patient_notes pn
        on s.patient_id = pn.patient_id
)

select * from final 