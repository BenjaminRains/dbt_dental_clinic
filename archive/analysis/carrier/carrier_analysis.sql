with source as (
    select * from {{ source('opendental', 'carrier') }}
),

renamed as (
    select
        -- Keys
        CarrierNum as carrier_id,
        CarrierGroupName as carrier_group_id,
        
        -- Carrier information
        CarrierName as carrier_name,
        TIN as tax_id_number,
        IsHidden as is_hidden,
        
        -- Location (minimizing full address details)
        City as city,
        State as state,
        Zip as zip,
        Country as country,
        
        -- Contact information
        Phone as phone,
        
        -- Electronic filing settings
        ElectID as electronic_id,
        NoSendElect as no_electronic_claims,
        TrustedEtransFlags as trusted_etrans_flags,
        EraAutomationOverride as era_automation_override,
        
        -- Canadian-specific settings
        IsCDA as is_canadian_dental_association,
        CDAnetVersion as cdanet_version,
        CanadianNetworkNum as canadian_network_id,
        CanadianEncryptionMethod as canadian_encryption_method,
        CanadianSupportedTypes as canadian_supported_types,
        
        -- Claim processing behavior
        IsCoinsuranceInverted as is_coinsurance_inverted,
        CobInsPaidBehaviorOverride as cob_ins_paid_behavior_override,
        OrthoInsPayConsolidate as ortho_ins_pay_consolidate,
        
        -- Display settings
        ApptTextBackColor as appointment_text_back_color,
        
        -- Metadata
        SecUserNumEntry as created_by_user_id,
        SecDateEntry as created_at,
        SecDateTEdit as updated_at
    from source
)

select * from renamed