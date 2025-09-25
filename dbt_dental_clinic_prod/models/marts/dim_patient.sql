{{
    config(
        materialized='table',
        schema='marts',
        unique_key='patient_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['patient_id'], 'unique': true},
            {'columns': ['_updated_at']},
            {'columns': ['primary_provider_id']},
            {'columns': ['clinic_id']},
            {'columns': ['patient_status']},
            {'columns': ['birth_date']}
        ]
    )
}}

/*
    Dimension model for Patient
    Part of System A: Core Data Foundation
    
    This model:
    1. Provides comprehensive patient demographic and clinical information
    2. Supports patient relationship management and family linkages
    3. Enables financial analysis and balance tracking
    4. Facilitates clinical workflow and care coordination
    
    Business Logic Features:
    - Demographics: age, gender, language, contact preferences
    - Clinical: disease tracking, document management, provider relationships
    - Financial: balance aging, insurance status, payment preferences
    - Relationships: family links, guarantor relationships, provider assignments
    
    Key Metrics:
    - Patient demographics and segmentation
    - Financial balance and aging analysis
    - Clinical risk indicators and disease management
    - Provider assignment and care coordination
    
    Data Quality Notes:
    - Patient status codes mapped to business-friendly descriptions
    - Age calculated from birth_date for consistency
    - Balance fields aggregated from financial transactions
    - Disease and document counts from related tables
    
    Performance Considerations:
    - Indexed on patient_id for optimal join performance
    - Provider and clinic indexes for common filtering patterns
    - Status and date indexes for analytical queries
    
    Dependencies:
    - stg_opendental__patient: Core patient data
    - stg_opendental__patientnote: Patient notes and emergency contacts
    - stg_opendental__patientlink: Family and relationship linkages
    - stg_opendental__disease: Active disease conditions
    - stg_opendental__document: Patient document management
*/

with source_patient as (
    select * from {{ ref('stg_opendental__patient') }}
),

patient_notes as (
    select 
        patient_id,
        medical as medical_notes,
        treatment as treatment_notes,
        family_financial as financial_notes,
        ice_name as emergency_contact_name,
        ice_phone as emergency_contact_phone
    from {{ ref('stg_opendental__patientnote') }}
),

patient_links as (
    select 
        patient_id_from as patient_id,
        array_agg(patient_id_to::text) as linked_patient_ids,
        array_agg(link_type::text) as link_types
    from {{ ref('stg_opendental__patientlink') }}
    group by patient_id_from
),

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

patient_documents as (
    select 
        patient_id,
        count(*) as document_count,
        array_agg(document_category_id::text) as document_categories
    from {{ ref('stg_opendental__document') }}
    group by patient_id
),

patient_enhanced as (
    select
        -- Primary identification
        s.patient_id,  -- Unique identifier for each patient
        
        -- Demographics
        s.middle_initial,  -- Patient's middle initial
        s.preferred_name,  -- Patient's preferred name
        case s.gender
            when 0 then 'M'
            when 1 then 'F'
            when 2 then 'O'
            else 'U'
        end as gender,  -- M=Male, F=Female, O=Other, U=Unknown
        s.language,  -- Patient's preferred language
        s.birth_date,  -- Patient's date of birth
        s.age,  -- Calculated age in years
        
        -- Age categories for business analysis
        case 
            when s.age < 18 then 'Minor'
            when s.age between 18 and 64 then 'Adult'
            when s.age >= 65 then 'Senior'
            else 'Unknown'
        end as age_category,
        
        -- Status and Classification
        case s.patient_status
            when 0 then 'Patient'
            when 1 then 'NonPatient'
            when 2 then 'Inactive'
            when 3 then 'Archived'
            when 4 then 'Deceased'
            when 5 then 'Deleted'
            else 'Unknown'
        end as patient_status,  -- Patient status description
        case s.position_code
            when 0 then 'Default'
            when 1 then 'House'
            when 2 then 'Staff'
            when 3 then 'VIP'
            when 4 then 'Other'
            else 'Unknown'
        end as position_code,  -- Position code description
        s.student_status,  -- Student status if applicable
        case s.urgency
            when false then 'Normal'
            when true then 'High'
            else 'Unknown'
        end as urgency,  -- Urgency level description
        s.premedication_required,  -- Boolean flag for premedication requirement
        
        -- Contact Preferences
        case s.preferred_confirmation_method
            when 0 then 'None'
            when 2 then 'Email'
            when 4 then 'Text'
            when 8 then 'Phone'
            else 'Unknown'
        end as preferred_confirmation_method,  -- Preferred confirmation method
        case s.preferred_contact_method
            when 0 then 'None'
            when 2 then 'Email'
            when 3 then 'Mail'
            when 4 then 'Phone'
            when 5 then 'Text'
            when 6 then 'Other'
            when 8 then 'Portal'
            else 'Unknown'
        end as preferred_contact_method,  -- Preferred contact method
        case s.preferred_recall_method
            when 0 then 'None'
            when 2 then 'Email'
            when 4 then 'Text'
            when 8 then 'Phone'
            else 'Unknown'
        end as preferred_recall_method,  -- Preferred recall method
        s.text_messaging_consent,  -- Boolean flag for text messaging consent
        s.prefer_confidential_contact,  -- Boolean flag for confidential contact preference
        
        -- Financial Information
        s.estimated_balance,  -- Estimated current balance
        s.total_balance,  -- Total outstanding balance
        s.balance_0_30_days,  -- Balance aged 0-30 days
        s.balance_31_60_days,  -- Balance aged 31-60 days
        s.balance_61_90_days,  -- Balance aged 61-90 days
        s.balance_over_90_days,  -- Balance aged over 90 days
        s.insurance_estimate,  -- Estimated insurance portion
        s.payment_plan_due,  -- Amount due under payment plan
        s.has_insurance_flag,  -- Boolean flag for insurance status
        s.billing_cycle_day,  -- Day of month for billing cycle (1-31)
        
        -- Balance status for business analysis
        case 
            when s.estimated_balance = 0 then 'No Balance'
            when s.estimated_balance > 0 then 'Outstanding Balance'
            else 'Credit Balance'
        end as balance_status,
        
        -- Important Dates
        s.first_visit_date,  -- Date of patient's first visit
        s.deceased_datetime,  -- Date and time of death if applicable
        s.admit_date,  -- Date patient was admitted
        
        -- Relationships
        s.guarantor_id,  -- ID of the guarantor
        s.primary_provider_id,  -- ID of the primary provider
        s.secondary_provider_id,  -- ID of the secondary provider
        s.clinic_id,  -- ID of the clinic
        s.fee_schedule_id,  -- ID of the fee schedule
        
        -- Patient Notes
        pn.medical_notes,  -- General medical notes
        pn.treatment_notes,  -- Notes related to treatment
        pn.financial_notes,  -- Notes related to financial matters
        pn.emergency_contact_name,  -- Name of emergency contact
        pn.emergency_contact_phone,  -- Phone number of emergency contact
        
        -- Patient Links
        pl.linked_patient_ids,  -- Array of linked patient IDs
        pl.link_types,  -- Array of relationship types
        
        -- Patient Diseases
        pd.disease_count,  -- Count of active diseases
        pd.disease_ids,  -- Array of disease definition IDs
        pd.disease_statuses,  -- Array of disease statuses
        
        -- Patient Documents
        doc.document_count,  -- Count of documents
        doc.document_categories,  -- Array of document category IDs
        
        -- Metadata
        s._loaded_at,  -- When ETL pipeline loaded the data
        s._created_at,  -- When record was created in source
        s._updated_at,  -- When record was last updated
        current_timestamp as _transformed_at,  -- dbt processing time
        current_timestamp as _mart_refreshed_at  -- Mart refresh time

    from source_patient s
    left join patient_notes pn
        on s.patient_id = pn.patient_id
    left join patient_links pl
        on s.patient_id = pl.patient_id
    left join patient_diseases pd
        on s.patient_id = pd.patient_id
    left join patient_documents doc
        on s.patient_id = doc.patient_id
),

final as (
    select * from patient_enhanced
)

select * from final