with source as (
    select * from {{ source('opendental', 'rxdef') }}
),

renamed as (
    select
        -- Primary Key
        "RxDefNum" as rx_def_num,
        
        -- Attributes
        "Drug" as drug,
        "Sig" as sig,
        "Disp" as disp,
        "Refills" as refills,
        "Notes" as notes,
        "IsControlled" as is_controlled,
        "RxCui" as rx_cui,
        "IsProcRequired" as is_proc_required,
        "PatientInstruction" as patient_instruction
    
    from source
)

select * from renamed
