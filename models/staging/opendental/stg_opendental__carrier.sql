{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'carrier') }}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"CarrierNum"', 'target': 'carrier_id'}
        ]) }},
        
        -- Basic Information
        "CarrierName" as carrier_name,
        "Address" as address_line1,
        "Address2" as address_line2,
        "City" as city,
        "State" as state,
        "Zip" as zip_code,
        "Phone" as phone,
        "TIN" as tax_id_number,
        
        -- Electronic Claims
        "ElectID" as electronic_id,
        {{ convert_opendental_boolean('"NoSendElect"') }} as no_send_electronic,
        
        -- Canadian Specific
        {{ convert_opendental_boolean('"IsCDA"') }} as is_canadian_dental_association,
        "CDAnetVersion" as cdanet_version,
        {{ transform_id_columns([
            {'source': '"CanadianNetworkNum"', 'target': 'canadian_network_id'}
        ]) }},
        "CanadianEncryptionMethod" as canadian_encryption_method,
        "CanadianSupportedTypes" as canadian_supported_types,
        
        -- Group and Display
        "CarrierGroupName" as carrier_group_name,
        "ApptTextBackColor" as appointment_text_background_color,
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        
        -- Insurance Behavior Settings
        {{ convert_opendental_boolean('"IsCoinsuranceInverted"') }} as is_coinsurance_inverted,
        {{ convert_opendental_boolean('"TrustedEtransFlags"') }} as trusted_etrans_flags,
        "CobInsPaidBehaviorOverride" as cob_insurance_paid_behavior_override,
        {{ convert_opendental_boolean('"EraAutomationOverride"') }} as era_automation_override,
        {{ convert_opendental_boolean('"OrthoInsPayConsolidate"') }} as ortho_insurance_payment_consolidate,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}
        
    from source_data
)

select * from renamed_columns
