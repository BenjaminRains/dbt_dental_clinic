{{ config(
    materialized='incremental',
    unique_key='rx_id'
) }}

with source_data as (
    select * from {{ source('opendental', 'rxpat') }}
    {% if is_incremental() %}
        where "DateTStamp" > (select max(_updated_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"RxNum"', 'target': 'rx_id'}
        ]) }},
        
        -- Foreign Keys
        {{ transform_id_columns([
            {'source': '"PatNum"', 'target': 'patient_id'},
            {'source': '"ProvNum"', 'target': 'provider_id'},
            {'source': '"PharmacyNum"', 'target': 'pharmacy_id'},
            {'source': '"ProcNum"', 'target': 'procedure_id'},
            {'source': '"ClinicNum"', 'target': 'clinic_id'},
            {'source': '"UserNum"', 'target': 'user_id'}
        ]) }},
        
        -- Dates
        {{ clean_opendental_date('"RxDate"') }} as rx_date,
        
        -- Prescription Details
        "Drug" as drug_name,
        "Sig" as sig,
        "Disp" as dispense_instructions,
        "Refills" as refills,
        "Notes" as notes,
        {{ convert_opendental_boolean('"IsControlled"') }} as is_controlled,
        "SendStatus" as send_status,
        "RxCui" as rx_cui,
        "DosageCode" as dosage_code,
        "DaysOfSupply" as days_of_supply,
        "PatientInstruction" as patient_instruction,
        
        -- E-Prescription Related
        "ErxGuid" as erx_guid,
        {{ convert_opendental_boolean('"IsErxOld"') }} as is_erx_old,
        "ErxPharmacyInfo" as erx_pharmacy_info,
        {{ convert_opendental_boolean('"IsProcRequired"') }} as is_proc_required,
        "RxType" as rx_type,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
