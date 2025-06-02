{{ config(
    materialized='table',
    description='Dimension table containing standardized patient information'
) }}

with Source as (
    select * from {{ ref('stg_opendental__patient') }}
),

PatientNotes as (
    select 
        patient_id,
        medical as medical_notes,
        treatment as treatment_notes,
        family_financial as financial_notes,
        ice_name as emergency_contact_name,
        ice_phone as emergency_contact_phone
    from {{ ref('stg_opendental__patientnote') }}
),

PatientLinks as (
    select 
        patient_id_from as patient_id,
        array_agg(patient_id_to) as linked_patient_ids,
        array_agg(link_type) as link_types
    from {{ ref('stg_opendental__patientlink') }}
    group by patient_id_from
),

PatientDiseases as (
    select 
        patient_id,
        count(*) as disease_count,
        array_agg(disease_def_id) as disease_ids,
        array_agg(problem_status) as disease_statuses
    from {{ ref('stg_opendental__disease') }}
    where date_stop is null  -- Only active diseases
    group by patient_id
),

PatientDocuments as (
    select 
        patient_id,
        count(*) as document_count,
        array_agg(document_category_id) as document_categories
    from {{ ref('stg_opendental__document') }}
    group by patient_id
),

Final as (
    select
        -- Primary Key
        s.patient_id,
        
        -- Demographics
        s.middle_initial,
        s.preferred_name,
        s.gender,
        s.language,
        s.birth_date,
        s.age,
        
        -- Status and Classification
        s.patient_status,
        s.position_code,
        s.student_status,
        s.urgency,
        s.premedication_required,
        
        -- Contact Preferences
        s.preferred_confirmation_method,
        s.preferred_contact_method,
        s.preferred_recall_method,
        s.text_messaging_consent,
        s.prefer_confidential_contact,
        
        -- Financial Information
        s.estimated_balance,
        s.total_balance,
        s.balance_0_30_days,
        s.balance_31_60_days,
        s.balance_61_90_days,
        s.balance_over_90_days,
        s.insurance_estimate,
        s.payment_plan_due,
        s.has_insurance_flag,
        s.billing_cycle_day,
        
        -- Important Dates
        s.first_visit_date,
        s.deceased_datetime,
        s.admit_date,
        
        -- Relationships
        s.guarantor_id,
        s.primary_provider_id,
        s.secondary_provider_id,
        s.clinic_id,
        s.fee_schedule_id,
        
        -- Patient Notes
        pn.medical_notes,
        pn.treatment_notes,
        pn.financial_notes,
        pn.emergency_contact_name,
        pn.emergency_contact_phone,
        
        -- Patient Links
        pl.linked_patient_ids,
        pl.link_types,
        
        -- Patient Diseases
        pd.disease_count,
        pd.disease_ids,
        pd.disease_statuses,
        
        -- Patient Documents
        doc.document_count,
        doc.document_categories,
        
        -- Metadata
        s._loaded_at,
        s._created_at,
        s._updated_at

    from Source s
    left join PatientNotes pn
        on s.patient_id = pn.patient_id
    left join PatientLinks pl
        on s.patient_id = pl.patient_id
    left join PatientDiseases pd
        on s.patient_id = pd.patient_id
    left join PatientDocuments doc
        on s.patient_id = doc.patient_id
)

select * from Final
