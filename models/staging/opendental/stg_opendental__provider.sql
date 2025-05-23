with source as (
    select * from {{ source('opendental', 'provider') }}
),

staged as (
    select
        -- Primary key
        -- Note: provider_id = 0 is a special case used for system-generated communications
        -- including automated notifications, system messages, and staff-initiated communications
        "ProvNum" as provider_id,
        
        -- Provider identifiers
        "Abbr" as provider_abbreviation,
        "ItemOrder" as provider_item_order,
        "LName" as provider_last_name,
        "FName" as provider_first_name,
        "MI" as provider_middle_initial,
        "Suffix" as provider_suffix,
        "PreferredName" as provider_preferred_name,
        "CustomID" as provider_custom_id,
        
        -- Provider classifications
        "FeeSched" as fee_schedule_id,
        "Specialty" as specialty_id,
        "ProvStatus" as provider_status,
        "AnesthProvType" as anesthesia_provider_type,
        "SchoolClassNum" as school_class_number,
        "EhrMuStage" as ehr_mu_stage,
        "ProvNumBillingOverride" as provider_billing_override_id,
        
        -- Provider identifiers and numbers
        "SSN" as ssn,
        "StateLicense" as state_license,
        "DEANum" as dea_number,
        "BlueCrossID" as blue_cross_id,
        "MedicaidID" as medicaid_id,
        "NationalProvID" as national_provider_id,
        "CanadianOfficeNum" as canadian_office_number,
        "EcwID" as ecw_id,
        "StateRxID" as state_rx_id,
        "StateWhereLicensed" as state_where_licensed,
        
        -- Taxonomy and classification
        "TaxonomyCodeOverride" as taxonomy_code_override,
        
        -- UI and display properties
        "ProvColor" as provider_color,
        "OutlineColor" as outline_color,
        "SchedNote" as schedule_note,
        "WebSchedDescript" as web_schedule_description,
        "WebSchedImageLocation" as web_schedule_image_location,
        
        -- Financial and goals
        "HourlyProdGoalAmt" as hourly_production_goal_amount,
        
        -- Boolean flags (keeping as smallint to match source)
        "IsSecondary" as is_secondary,
        "IsHidden" as is_hidden,
        "UsingTIN" as is_using_tin,
        "SigOnFile" as has_signature_on_file,
        "IsCDAnet" as is_cdanet,
        "IsNotPerson" as is_not_person,
        "IsInstructor" as is_instructor,
        "IsHiddenReport" as is_hidden_report,
        "IsErxEnabled" as is_erx_enabled,
        
        -- Dates
        "Birthdate" as birth_date,
        "DateTerm" as termination_date,
        "DateTStamp" as record_updated_at,
        
        -- Relations
        "EmailAddressNum" as email_address_id,
        
        -- Required metadata columns
        current_timestamp as _loaded_at,
        source."DateTStamp" as _created_at,
        source."DateTStamp" as _updated_at

    from source
)

select * from staged
