with source as (
    select * from {{ source('opendental', 'rxdef') }}
),

renamed as (
    select
        -- Primary key
        RxDefNum as prescription_definition_id,

        -- Drug information
        Drug as drug_name,
        RxCui as rx_cui_code,

        -- Prescription details
        Sig as sig_instructions,
        Disp as dispense_instructions,
        Refills as refill_instructions,

        -- Status flags
        IsControlled as is_controlled_substance_flag,
        IsProcRequired as is_procedure_required_flag,

        -- Instructions and notes
        Notes as prescriber_notes,
        PatientInstruction as patient_instructions
    from source
)

select * from renamed