{{ config(
    materialized='table',
    schema='intermediate',
    unique_key='clinic_id',
    indexes=[
        {'columns': ['clinic_id'], 'unique': true},
        {'columns': ['is_active_clinic']},
        {'columns': ['clinic_name']}
    ],
    tags=['foundation']
) }}

/*
    Intermediate model for clinic profile
    Part of System Foundation: Clinic/Location Management
    
    This model:
    1. Transforms clinic data into business-friendly attributes
    2. Applies business rules for clinic status and configuration
    3. Creates derived fields for clinic capabilities
    4. Establishes foundation for multi-location reporting
    
    Business Logic Features:
    - Clinic Status Classification: Active, hidden, medical-only, existing patients only
    - Configuration Flags: Billing, confirmation, SMS, prescription settings
    - Address Management: Physical, billing, and pay-to addresses
    - Operational Settings: Timezone, scheduling notes, place of service
    
    Data Sources:
    - stg_opendental__clinic: Primary clinic/location data
    
    Data Quality Notes:
    - Hidden clinics retained for historical data
    - Missing abbreviations default to first 10 chars of clinic name
    - Separate billing/pay-to addresses properly identified
    - SMS settings indicate active messaging capability
    
    Performance Considerations:
    - Table materialization for fast lookups
    - Indexed on clinic_id, is_active_clinic, and clinic_name
    - Efficient boolean flag calculations
*/

with clinic_base as (
    select * from {{ ref('stg_opendental__clinic') }}
),

clinic_enhanced as (
    select
        -- Core identifiers
        clinic_id,
        clinic_name,
        clinic_abbreviation,
        display_order,
        
        -- Contact information
        address_line_1,
        address_line_2,
        city,
        state,
        zip_code,
        phone_number,
        fax_number,
        email_alias,
        
        -- Billing information
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
        
        -- Configuration flags
        default_place_of_service,
        is_medical_only,
        use_billing_address_on_claims,
        is_insurance_verification_excluded,
        is_confirmation_enabled,
        is_confirmation_default,
        is_new_patient_appointment_excluded,
        is_hidden,
        has_procedure_on_prescription,
        
        -- Additional settings
        timezone,
        scheduling_note,
        medical_lab_account_number,
        sms_contract_date,
        sms_monthly_limit,
        
        -- Foreign keys
        email_address_id,
        default_provider_id,
        insurance_billing_provider_id,
        region_id,
        external_id,
        
        -- Business logic flags
        case 
            when is_hidden = false then true
            else false
        end as is_active_clinic,
        
        case
            when billing_address_line_1 is not null 
            and billing_address_line_1 != address_line_1
            then true
            else false
        end as has_separate_billing_address,
        
        case
            when pay_to_address_line_1 is not null
            and pay_to_address_line_1 != address_line_1
            then true
            else false
        end as has_separate_payto_address,
        
        case
            when is_confirmation_enabled = true 
            and is_confirmation_default = true
            then true
            else false
        end as uses_appointment_confirmations,
        
        case
            when sms_contract_date is not null
            and sms_monthly_limit > 0
            then true
            else false
        end as has_sms_enabled,
        
        -- Clinic categorization
        case 
            when is_hidden = true then 'Hidden'
            when is_medical_only = true then 'Medical Only'
            when is_new_patient_appointment_excluded = true then 'Existing Patients Only'
            else 'Active'
        end as clinic_status,
        
        case
            when is_medical_only = true then 'Medical'
            else 'Dental'
        end as clinic_type,
        
        -- Metadata
        _loaded_at
        
    from clinic_base
),

final as (
    select
        -- Core information
        clinic_id,
        clinic_name,
        coalesce(clinic_abbreviation, left(clinic_name, 10)) as clinic_abbreviation,
        display_order,
        
        -- Contact information
        address_line_1,
        address_line_2,
        city,
        state,
        zip_code,
        phone_number,
        fax_number,
        email_alias,
        
        -- Full address concatenation
        concat_ws(', ',
            address_line_1,
            case when address_line_2 is not null and trim(address_line_2) != '' 
                then address_line_2 end,
            city,
            state,
            zip_code
        ) as full_address,
        
        -- Billing information
        bank_number,
        billing_address_line_1,
        billing_address_line_2,
        billing_city,
        billing_state,
        billing_zip,
        concat_ws(', ',
            billing_address_line_1,
            case when billing_address_line_2 is not null and trim(billing_address_line_2) != ''
                then billing_address_line_2 end,
            billing_city,
            billing_state,
            billing_zip
        ) as full_billing_address,
        pay_to_address_line_1,
        pay_to_address_line_2,
        pay_to_city,
        pay_to_state,
        pay_to_zip,
        concat_ws(', ',
            pay_to_address_line_1,
            case when pay_to_address_line_2 is not null and trim(pay_to_address_line_2) != ''
                then pay_to_address_line_2 end,
            pay_to_city,
            pay_to_state,
            pay_to_zip
        ) as full_payto_address,
        
        -- Configuration flags
        default_place_of_service,
        is_medical_only,
        use_billing_address_on_claims,
        is_insurance_verification_excluded,
        is_confirmation_enabled,
        is_confirmation_default,
        is_new_patient_appointment_excluded,
        is_hidden,
        has_procedure_on_prescription,
        
        -- Additional settings
        timezone,
        scheduling_note,
        medical_lab_account_number,
        sms_contract_date,
        sms_monthly_limit,
        
        -- Foreign keys
        email_address_id,
        default_provider_id,
        insurance_billing_provider_id,
        region_id,
        external_id,
        
        -- Business logic flags
        is_active_clinic,
        has_separate_billing_address,
        has_separate_payto_address,
        uses_appointment_confirmations,
        has_sms_enabled,
        
        -- Categorization
        clinic_status,
        clinic_type,
        
        -- Metadata
        {{ standardize_intermediate_metadata(
            primary_source_alias='ce',
            source_metadata_fields=['_loaded_at']
        ) }}
        
    from clinic_enhanced ce
)

select * from final

