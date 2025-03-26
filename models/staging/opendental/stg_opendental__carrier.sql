{{ config(
    materialized='view'
) }}

with source as (
    select * from {{ source('opendental', 'carrier') }}
    where "SecDateTEdit" >= '2023-01-01'
),

renamed as (
    select
        -- Primary Key
        "CarrierNum" as carrier_id,
        
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
        ("NoSendElect" = 1)::boolean as no_send_electronic,
        
        -- Canadian Specific
        ("IsCDA" = 1)::boolean as is_canadian_dental_association,
        "CDAnetVersion" as cdanet_version,
        "CanadianNetworkNum" as canadian_network_num,
        "CanadianEncryptionMethod" as canadian_encryption_method,
        "CanadianSupportedTypes" as canadian_supported_types,
        
        -- Group and Display
        "CarrierGroupName" as carrier_group_id,
        "ApptTextBackColor" as appointment_text_background_color,
        ("IsHidden" = 1)::boolean as is_hidden,
        
        -- Insurance Behavior Settings
        ("IsCoinsuranceInverted" = 1)::boolean as is_coinsurance_inverted,
        ("TrustedEtransFlags" = 1)::boolean as trusted_etrans_flags,
        "CobInsPaidBehaviorOverride" as cob_insurance_paid_behavior_override,
        ("EraAutomationOverride" = 1)::boolean as era_automation_override,
        ("OrthoInsPayConsolidate" = 1)::boolean as ortho_insurance_payment_consolidate,
        
        -- Metadata
        "SecUserNumEntry" as user_entry_id,
        "SecDateEntry" as created_at,
        "SecDateTEdit" as updated_at
        
    from source
)

select * from renamed
