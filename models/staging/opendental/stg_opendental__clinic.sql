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
    materialized='view',
    schema='staging'
) }}

with source as (
    select * from {{ source('opendental', 'clinic') }}
),

renamed as (
    select
        -- Primary Key
        "ClinicNum" as clinic_id,
        
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
        "EmailAddressNum" as email_address_id,
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
        
        -- Provider References
        "DefaultProv" as default_provider_id,
        "InsBillingProv" as insurance_billing_provider_id,
        
        -- Configuration Flags
        "DefaultPlaceService" as default_place_of_service,
        "IsMedicalOnly" as is_medical_only,
        "UseBillAddrOnClaims" as use_billing_address_on_claims,
        "IsInsVerifyExcluded" as is_insurance_verification_excluded,
        "IsConfirmEnabled" as is_confirmation_enabled,
        "IsConfirmDefault" as is_confirmation_default,
        "IsNewPatApptExcluded" as is_new_patient_appointment_excluded,
        "IsHidden" as is_hidden,
        "HasProcOnRx" as has_procedure_on_prescription,
        
        -- Additional Settings
        "Region" as region_id,
        "TimeZone" as timezone,
        "SchedNote" as scheduling_note,
        "MedLabAccountNum" as medical_lab_account_number,
        "ExternalID" as external_id,
        
        -- SMS Settings
        "SmsContractDate" as sms_contract_date,
        "SmsMonthlyLimit" as sms_monthly_limit,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        "date_entry" as _created_at,      -- Rename source creation timestamp
        coalesce("date_tstamp", "date_entry") as _updated_at  -- Rename source update timestamp

    from source
)

select * from renamed 