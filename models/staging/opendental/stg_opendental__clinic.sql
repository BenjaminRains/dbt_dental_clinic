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
        
        -- Metadata
        _loaded_at,  -- Added by ETL pipeline when loading to raw schema
        "DateEntry" as _created_at,  -- Transformed from source creation timestamp
        coalesce("DateTStamp", "DateEntry") as _updated_at  -- Transformed from source update timestamp

    from source
)

select * from renamed 