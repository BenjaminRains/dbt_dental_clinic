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
    - stg_opendental__clinic: Staged clinic data from OpenDental
    
    Performance Considerations:
    - Table materialization for optimal query performance
    - Indexed on key lookup columns
    - Includes only essential attributes for analysis
*/

with clinic_data as (
    select
        clinic_id,
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
        
        -- Billing Information
        bank_number,
        billing_address_line_1,
        billing_address_line_2,
        billing_city,
        billing_state,
        billing_zip,
        pay_to_address_line_1,
        pay_to_address_line_2,
        pay_to_city,
        pay_to_state,
        pay_to_zip,
        
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
        
        -- Metadata (from staging table)
        _loaded_at
        
    from {{ ref('stg_opendental__clinic') }}
    where clinic_id is not null
),

-- Add business logic and derived fields
final as (
    select
        -- Primary key
        clinic_id,
        
        -- Basic Information
        clinic_name,
        coalesce(clinic_abbreviation, left(clinic_name, 10)) as clinic_abbreviation,
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
        
        -- Billing Information
        bank_number,
        billing_address_line_1,
        billing_address_line_2,
        billing_city,
        billing_state,
        billing_zip,
        pay_to_address_line_1,
        pay_to_address_line_2,
        pay_to_city,
        pay_to_state,
        pay_to_zip,
        
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
        
        -- Derived Fields
        case 
            when is_hidden = true then 'Hidden'
            when is_medical_only = true then 'Medical Only'
            else 'Active'
        end as clinic_status,
        
        case 
            when billing_address_line_1 is not null then 'Separate Billing'
            else 'Same as Clinic'
        end as billing_address_type,
        
        -- Standardized mart metadata (using source alias, only _loaded_at available)
        {{ standardize_mart_metadata(
            primary_source_alias='clinic_data',
            source_metadata_fields=['_loaded_at']
        ) }}
        
    from clinic_data
)

select * from final
