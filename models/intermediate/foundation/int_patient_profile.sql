{{
    config(
        materialized='table',
        tags=['foundation', 'daily'],
        description='''
            # int_patient_profile

            This foundation model combines core patient information from various staging models to create a comprehensive patient profile.

            Key information included:
            - Basic demographics and contact information
            - Family relationships
            - Geographic location
            - Emergency contacts
            - Medical notes and treatment information
            - Patient preferences (pronouns, consent)

            ## Usage
            This model serves as a foundation for downstream patient-related analytics and should be used as the primary source of patient profile information.

            ## Materialization
            Table materialization is used due to the frequent access of this data by downstream models.

            ## Notes:
            - Geographic/location data (city, state, zipcode) is currently not included as the relationship between
              patient and zipcode tables needs to be investigated
            - Consider checking for additional patient address or location tables in the source system
        '''
    )
}}

WITH patient_base AS (
    SELECT
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
        birth_date as birthdate,
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
        created_at,
        updated_at
    FROM {{ ref('stg_opendental__patient') }}
),

patient_family AS (
    SELECT
        patient_id_from as patient_id,
        patient_id_to as family_id,
        link_type,
        linked_at
    FROM {{ ref('stg_opendental__patientlink') }}
),

patient_notes AS (
    SELECT
        patient_id,
        ice_name,
        ice_phone,
        medical,
        treatment,
        pronoun,
        consent,
        created_at as notes_created_at,
        updated_at as notes_updated_at
    FROM {{ ref('stg_opendental__patientnote') }}
)

SELECT
    p.patient_id,
    p.guarantor_id,
    p.primary_provider_id,
    p.secondary_provider_id,
    p.clinic_id,
    p.preferred_name,
    p.middle_initial,
    p.gender,
    p.language,
    p.birthdate,
    p.age,
    p.patient_status,
    p.position_code,
    p.student_status,
    p.preferred_confirmation_method,
    p.preferred_contact_method,
    p.text_messaging_consent,
    p.estimated_balance,
    p.total_balance,
    p.has_insurance_flag,
    pf.family_id,
    pf.link_type as family_link_type,
    pf.linked_at as family_linked_at,
    pn.ice_name as emergency_contact_name,
    pn.ice_phone as emergency_contact_phone,
    pn.medical as medical_notes,
    pn.treatment as treatment_notes,
    pn.pronoun,
    pn.consent,
    p.first_visit_date,
    p.created_at as patient_created_at,
    p.updated_at as patient_updated_at,
    pn.notes_created_at,
    pn.notes_updated_at,
    CURRENT_TIMESTAMP as model_created_at,
    CURRENT_TIMESTAMP as model_updated_at
FROM patient_base p
LEFT JOIN patient_family pf 
    ON p.patient_id = pf.patient_id
LEFT JOIN patient_notes pn
    ON p.patient_id = pn.patient_id 