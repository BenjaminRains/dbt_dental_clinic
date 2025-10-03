{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'rxdef') }}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"RxDefNum"', 'target': 'rx_def_id'}
        ]) }},
        
        -- Attributes
        "Drug" as drug,
        "Sig" as sig,
        "Disp" as disp,
        "Refills" as refills,
        "Notes" as notes,
        {{ convert_opendental_boolean('"IsControlled"') }} as is_controlled,
        "RxCui" as rx_cui,
        {{ convert_opendental_boolean('"IsProcRequired"') }} as is_proc_required,
        "PatientInstruction" as patient_instruction,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
    
    from source_data
)

select * from renamed_columns
