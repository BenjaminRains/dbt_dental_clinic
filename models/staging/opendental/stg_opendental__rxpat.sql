with source as (
    select * from {{ source('opendental', 'rxpat') }}
),

renamed as (
    select
        -- Primary Key
        "RxNum" as rx_id,
        
        -- Foreign Keys
        "PatNum" as patient_id,
        "ProvNum" as provider_id,
        "PharmacyNum" as pharmacy_id,
        "ProcNum" as procedure_id,
        "ClinicNum" as clinic_id,
        "UserNum" as user_id,
        
        -- Timestamps and Dates
        "RxDate" as rx_date,
        "DateTStamp" as date_timestamp,
        
        -- Prescription Details
        "Drug" as drug_name,
        "Sig" as sig,
        "Disp" as dispense_instructions,
        "Refills" as refills,
        "Notes" as notes,
        CASE WHEN "IsControlled" = 1 THEN true ELSE false END as is_controlled,
        "SendStatus" as send_status,
        "RxCui" as rx_cui,
        "DosageCode" as dosage_code,
        "DaysOfSupply" as days_of_supply,
        "PatientInstruction" as patient_instruction,
        
        -- E-Prescription Related
        "ErxGuid" as erx_guid,
        CASE WHEN "IsErxOld" = 1 THEN true ELSE false END as is_erx_old,
        "ErxPharmacyInfo" as erx_pharmacy_info,
        CASE WHEN "IsProcRequired" = 1 THEN true ELSE false END as is_proc_required,
        "RxType" as rx_type

    from source
)

select * from renamed
