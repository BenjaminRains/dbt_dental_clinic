{{ config(
    materialized='incremental',
    unique_key='provider_id',
    on_schema_change='sync_all_columns'
) }}

with source_data as (
    select * from {{ source('opendental', 'provider') }}
    {% if is_incremental() %}
    where {{ clean_opendental_date('"DateTStamp"') }} > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"ProvNum"', 'target': 'provider_id'},
            {'source': '"FeeSched"', 'target': 'fee_schedule_id'},
            {'source': '"Specialty"', 'target': 'specialty_id'},
            {'source': '"SchoolClassNum"', 'target': 'school_class_id'},
            {'source': 'NULLIF("ProvNumBillingOverride", 0)', 'target': 'billing_override_provider_id'},
            {'source': 'NULLIF("EmailAddressNum", 0)', 'target': 'email_address_id'}
        ]) }},
        
        -- Provider identifiers and names
        "ItemOrder" as display_order,
        "CustomID" as custom_id,
        
        -- Professional identifiers
        "StateWhereLicensed" as state_where_licensed,
        "TaxonomyCodeOverride" as taxonomy_code_override,
        
        -- Classification and status
        "ProvStatus"::integer as provider_status,
        "AnesthProvType"::integer as anesthesia_provider_type,
        "EhrMuStage"::integer as ehr_mu_stage,
        
        -- UI and display properties
        "ProvColor" as provider_color,
        "OutlineColor" as outline_color,
        "SchedNote" as schedule_note,
        "WebSchedDescript" as web_schedule_description,
        "WebSchedImageLocation" as web_schedule_image_location,
        
        -- Financial goals
        "HourlyProdGoalAmt" as hourly_production_goal_amount,
        
        -- Boolean fields using macro
        {{ convert_opendental_boolean('"IsSecondary"') }} as is_secondary,
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"UsingTIN"') }} as is_using_tin,
        {{ convert_opendental_boolean('"SigOnFile"') }} as has_signature_on_file,
        {{ convert_opendental_boolean('"IsCDAnet"') }} as is_cdanet,
        {{ convert_opendental_boolean('"IsNotPerson"') }} as is_not_person,
        {{ convert_opendental_boolean('"IsInstructor"') }} as is_instructor,
        {{ convert_opendental_boolean('"IsHiddenReport"') }} as is_hidden_report,
        {{ convert_opendental_boolean('"IsErxEnabled"') }} as is_erx_enabled,
        
        -- Date fields using macro
        {{ clean_opendental_date('"Birthdate"') }} as birth_date,
        {{ clean_opendental_date('"DateTerm"') }} as termination_date,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
