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
    - int_patient_profile: Comprehensive intermediate model with patient demographics, notes, 
      family links, disease tracking, and document management
*/

with source_patient as (
    select * from {{ ref('int_patient_profile') }}
),

patient_enhanced as (
    select
        -- Primary identification
        s.patient_id,  -- Unique identifier for each patient
        
        -- Demographics (from intermediate model)
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
        
        -- Status and Classification (using intermediate model's status description)
        s.patient_status_description as patient_status,  -- Patient status description from intermediate
        s.patient_category as position_code,  -- Position code description from intermediate
        s.student_status,  -- Student status if applicable
        'Normal' as urgency,  -- Not available in intermediate, default to Normal
        false as premedication_required,  -- Not available in intermediate, default to false
        
        -- Contact Preferences (from intermediate model)
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
        null as preferred_recall_method,  -- Not available in intermediate
        s.text_messaging_consent,  -- Boolean flag for text messaging consent
        false as prefer_confidential_contact,  -- Not available in intermediate, default to false
        
        -- Financial Information (from intermediate model)
        s.estimated_balance,  -- Estimated current balance
        s.total_balance,  -- Total outstanding balance (from source - may be 0 if not populated)
        0.00 as balance_0_30_days,  -- Not available in intermediate, default to 0
        0.00 as balance_31_60_days,  -- Not available in intermediate, default to 0
        0.00 as balance_61_90_days,  -- Not available in intermediate, default to 0
        0.00 as balance_over_90_days,  -- Not available in intermediate, default to 0
        0.00 as insurance_estimate,  -- Not available in intermediate, default to 0
        0.00 as payment_plan_due,  -- Not available in intermediate, default to 0
        s.has_insurance_flag,  -- Boolean flag for insurance status
        0 as billing_cycle_day,  -- Not available in intermediate, default to 0
        
        -- Balance status for business analysis
        case 
            when s.estimated_balance = 0 then 'No Balance'
            when s.estimated_balance > 0 then 'Outstanding Balance'
            else 'Credit Balance'
        end as balance_status,
        
        -- Important Dates
        s.first_visit_date,  -- Date of patient's first visit (already cleaned in intermediate)
        null::timestamp as deceased_datetime,  -- Not available in intermediate
        null::date as admit_date,  -- Not available in intermediate
        
        -- Relationships (from intermediate model)
        s.guarantor_id,  -- ID of the guarantor
        s.primary_provider_id,  -- ID of the primary provider
        s.secondary_provider_id,  -- ID of the secondary provider
        s.clinic_id,  -- ID of the clinic
        s.fee_schedule_id,  -- ID of the fee schedule
        
        -- Patient Notes (from intermediate model)
        s.medical_notes,  -- General medical notes
        s.treatment_notes,  -- Notes related to treatment
        null as financial_notes,  -- Not available in intermediate (was family_financial in staging)
        
        -- Patient Links (from intermediate model - already aggregated)
        array_to_string(s.family_ids, ',') as linked_patient_ids,  -- Convert array to text for compatibility
        array_to_string(s.family_link_types::text[], ',') as link_types,  -- Convert array to text for compatibility
        
        -- Patient Diseases (from intermediate model - active diseases aggregated)
        s.disease_count,  -- Count of active diseases
        s.disease_ids,  -- Array of disease definition IDs
        s.disease_statuses,  -- Array of disease statuses
        
        -- Patient Documents (from intermediate model - documents aggregated)
        s.document_count,  -- Count of documents
        s.document_categories,  -- Array of document category IDs
        
        -- Metadata (from intermediate model)
        s._loaded_at,  -- When ETL pipeline loaded the data
        s._created_at,  -- When record was created in source
        s._updated_at,  -- When record was last updated
        current_timestamp as _transformed_at,  -- dbt processing time
        current_timestamp as _mart_refreshed_at  -- Mart refresh time

    from source_patient s
),

final as (
    select * from patient_enhanced
)

select * from final