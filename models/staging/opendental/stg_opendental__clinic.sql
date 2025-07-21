{#
    Note: This staging model is prepared for when data becomes available.
    Current Status (2024-03-14):
    - Database: opendental_analytics
    - Schema: public
    - Table: clinic
    - Status: No data available in local development environment
    - Expected Columns: See DDL in analysis/clinic/clinic_pg_ddl.sql
    
    The model is structured to handle:
    1. Standard column naming and transformations
    2. Metadata fields (_loaded_at, _created_at, _updated_at)
    3. All columns from the source DDL
    
    TODO: Once data is available:
    1. Verify all columns exist as expected
    2. Test data quality and transformations
    3. Update documentation with actual data examples
#}

{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'clinic') }}
),

renamed_columns as (
    select
        -- Primary Key and Foreign Keys
        {{ transform_id_columns([
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"EmailAddressNum"', 'target': 'email_address_id'},
            {'source': '"DefaultProv"', 'target': 'default_provider_id'},
            {'source': '"InsBillingProv"', 'target': 'insurance_billing_provider_id'},
            {'source': '"Region"', 'target': 'region_id'}
        ]) }},
        
        -- Basic Information
        "Description" as clinic_name,
        "Abbr" as clinic_abbreviation,
        "ItemOrder" as display_order,
        
        -- Contact Information
        "Address" as address_line_1,
        "Address2" as address_line_2,
        "City" as city,
        "State" as state,
        "Zip" as zip_code,
        "Phone" as phone_number,
        "Fax" as fax_number,
        "EmailAliasOverride" as email_alias,
        
        -- Billing Information
        "BankNumber" as bank_number,
        "BillingAddress" as billing_address_line_1,
        "BillingAddress2" as billing_address_line_2,
        "BillingCity" as billing_city,
        "BillingState" as billing_state,
        "BillingZip" as billing_zip,
        "PayToAddress" as pay_to_address_line_1,
        "PayToAddress2" as pay_to_address_line_2,
        "PayToCity" as pay_to_city,
        "PayToState" as pay_to_state,
        "PayToZip" as pay_to_zip,
        
        -- Configuration Flags (Boolean Fields)
        "DefaultPlaceService" as default_place_of_service,
        {{ convert_opendental_boolean('"IsMedicalOnly"') }} as is_medical_only,
        {{ convert_opendental_boolean('"UseBillAddrOnClaims"') }} as use_billing_address_on_claims,
        {{ convert_opendental_boolean('"IsInsVerifyExcluded"') }} as is_insurance_verification_excluded,
        {{ convert_opendental_boolean('"IsConfirmEnabled"') }} as is_confirmation_enabled,
        {{ convert_opendental_boolean('"IsConfirmDefault"') }} as is_confirmation_default,
        {{ convert_opendental_boolean('"IsNewPatApptExcluded"') }} as is_new_patient_appointment_excluded,
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"HasProcOnRx"') }} as has_procedure_on_prescription,
        
        -- Additional Settings
        "TimeZone" as timezone,
        "SchedNote" as scheduling_note,
        "MedLabAccountNum" as medical_lab_account_number,
        "ExternalID" as external_id,
        
        -- Date Fields
        {{ clean_opendental_date('"SmsContractDate"') }} as sms_contract_date,
        
        -- SMS Settings
        "SmsMonthlyLimit" as sms_monthly_limit,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateEntry"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns 
