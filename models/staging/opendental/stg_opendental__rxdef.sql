with source as (
    select * from {{ source('opendental', 'rxdef') }}
),

renamed as (
    select
        -- Primary Key
        "RxDefNum" as rx_def_id,
        
        -- Attributes
        "Drug" as drug,
        "Sig" as sig,
        "Disp" as disp,
        "Refills" as refills,
        "Notes" as notes,
        "IsControlled" as is_controlled,
        "RxCui" as rx_cui,
        "IsProcRequired" as is_proc_required,
        "PatientInstruction" as patient_instruction,
        
        -- Metadata
        current_timestamp as _loaded_at,  -- When ETL pipeline loaded the data
        null::timestamp as _created_at,   -- No creation date available in source
        current_timestamp as _updated_at  -- Last update is when we loaded it
    
    from source
)

select * from renamed
