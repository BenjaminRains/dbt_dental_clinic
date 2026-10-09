
with source as (
    select * from {{ source('opendental', 'provider') }}
),

renamed as (
    select
        -- Primary key
        ProvNum as provider_id,

        -- Provider identification
        Abbr as provider_abbreviation,
        LName as last_name,
        FName as first_name,
        MI as middle_initial,
        Suffix as name_suffix,
        PreferredName as preferred_name,

        -- Professional details
        FeeSched as fee_schedule_id,
        Specialty as specialty_id,
        AnesthProvType as anesthesia_provider_type,
        SchoolClassNum as school_class_id,

        -- Professional identifiers (sensitive)
        SSN as social_security_number,
        StateLicense as state_license_number,
        DEANum as dea_number,
        NationalProvID as npi_number,
        BlueCrossID as blue_cross_id,
        MedicaidID as medicaid_id,
        StateRxID as state_rx_id,
        CustomID as custom_identifier,
        CanadianOfficeNum as canadian_office_number,
        EcwID as ecw_identifier,

        -- Status and type flags
        IsSecondary as is_secondary_flag,
        IsHidden as is_hidden_flag,
        IsHiddenReport as is_hidden_report_flag,
        UsingTIN as using_tin_flag,
        SigOnFile as signature_on_file_flag,
        IsCDAnet as is_cdanet_flag,
        IsNotPerson as is_not_person_flag,
        IsInstructor as is_instructor_flag,
        IsErxEnabled as is_erx_enabled_flag,
        ProvStatus as provider_status,

        -- Visual settings
        ProvColor as provider_color,
        OutlineColor as outline_color,
        ItemOrder as display_order,

        -- Location and licensing
        StateWhereLicensed as licensed_state,
        TaxonomyCodeOverride as taxonomy_code_override,

        -- Electronic health record
        EhrMuStage as ehr_meaningful_use_stage,
        ProvNumBillingOverride as billing_provider_override_id,

        -- Scheduling and goals
        SchedNote as schedule_notes,
        WebSchedDescript as web_schedule_description,
        WebSchedImageLocation as web_schedule_image_location,
        HourlyProdGoalAmt as hourly_production_goal,

        -- Dates
        Birthdate as birth_date,
        DateTerm as termination_date,
        DateTStamp as updated_at,

        -- Additional relationships
        EmailAddressNum as email_address_id
    from source
)

select * from renamed