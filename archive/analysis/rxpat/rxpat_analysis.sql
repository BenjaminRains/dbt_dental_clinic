with source as (
    select * from {{ source('opendental', 'rxpat') }}
),

renamed as (
    select
        -- Primary key
        RxNum as prescription_id,

        -- Relationships
        PatNum as patient_id,
        ProvNum as provider_id,
        PharmacyNum as pharmacy_id,
        ClinicNum as clinic_id,
        UserNum as user_id,
        ProcNum as procedure_id,

        -- Prescription details (contains PHI)
        Drug as drug_name,
        Sig as sig_instructions,
        Disp as dispense_instructions,
        Refills as refill_instructions,
        DaysOfSupply as days_supply,
        DosageCode as dosage_code,
        RxCui as rx_cui_code,
        RxType as prescription_type,

        -- Status flags
        IsControlled as is_controlled_substance_flag,
        IsProcRequired as is_procedure_required_flag,
        SendStatus as send_status,

        -- E-prescribing information
        ErxGuid as erx_guid,
        IsErxOld as is_erx_old_flag,
        ErxPharmacyInfo as erx_pharmacy_info,

        -- Dates
        RxDate as prescription_date,
        DateTStamp as updated_at,

        -- Notes and instructions (may contain PHI)
        Notes as prescription_notes,
        PatientInstruction as patient_instructions
    from source
)

select * from renamed