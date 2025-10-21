{{ config(
    materialized='table',
    indexes=[
        {'columns': ['clinic_id'], 'unique': true},
        {'columns': ['clinic_name']},
        {'columns': ['is_hidden']}
    ]
) }}

/*
    Dimension table for clinic/location information
    Part of the marts layer - provides business-ready clinic dimensions
    
    This model:
    1. Creates a comprehensive dimension table for clinic analysis
    2. Standardizes clinic information across the organization
    3. Provides clinic hierarchy and configuration details
    4. Includes operational flags and contact information
    
    Business Logic Features:
    - Clinic Identification: Unique clinic identifiers with display names
    - Location Information: Complete address and contact details
    - Configuration Flags: Operational settings and preferences
    - Billing Configuration: Billing addresses and provider assignments
    
    Data Sources:
    - int_clinic_profile: Intermediate model with clinic data and business logic
    
    Performance Considerations:
    - Table materialization for optimal query performance
    - Indexed on key lookup columns
    - Business logic centralized in intermediate layer
    - Includes comprehensive clinic attributes and derived fields
*/

with clinic_data as (
    select * from {{ ref('int_clinic_profile') }}
),

-- Final mart selection (business logic already in intermediate)
final as (
    select
        -- Primary key
        clinic_id,
        
        -- Basic Information
        clinic_name,
        clinic_abbreviation,
        display_order,
        
        -- Contact Information
        address_line_1,
        address_line_2,
        city,
        state,
        zip_code,
        phone_number,
        fax_number,
        email_alias,
        full_address,
        
        -- Billing Information
        bank_number,
        billing_address_line_1,
        billing_address_line_2,
        billing_city,
        billing_state,
        billing_zip,
        full_billing_address,
        pay_to_address_line_1,
        pay_to_address_line_2,
        pay_to_city,
        pay_to_state,
        pay_to_zip,
        full_payto_address,
        
        -- Configuration Flags
        default_place_of_service,
        is_medical_only,
        use_billing_address_on_claims,
        is_insurance_verification_excluded,
        is_confirmation_enabled,
        is_confirmation_default,
        is_new_patient_appointment_excluded,
        is_hidden,
        has_procedure_on_prescription,
        
        -- Additional Settings
        timezone,
        scheduling_note,
        medical_lab_account_number,
        sms_contract_date,
        sms_monthly_limit,
        
        -- Foreign Keys
        email_address_id,
        default_provider_id,
        insurance_billing_provider_id,
        region_id,
        external_id,
        
        -- Business Logic Flags (from intermediate)
        is_active_clinic,
        has_separate_billing_address,
        has_separate_payto_address,
        uses_appointment_confirmations,
        has_sms_enabled,
        
        -- Categorization (from intermediate)
        clinic_status,
        clinic_type,
        
        -- Legacy field for compatibility
        case 
            when has_separate_billing_address then 'Separate Billing'
            else 'Same as Clinic'
        end as billing_address_type,
        
        -- Standardized mart metadata
        {{ standardize_mart_metadata(
            primary_source_alias='cd',
            source_metadata_fields=['_loaded_at', '_transformed_at']
        ) }}
        
    from clinic_data cd
)

select * from final
