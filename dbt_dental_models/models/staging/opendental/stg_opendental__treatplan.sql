{{ config(
    materialized='incremental',
    unique_key='treatment_plan_id'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'treatplan') }}
    where {{ clean_opendental_date('"DateTP"') }} >= '2023-01-01'
    {% if is_incremental() %}
        and {{ clean_opendental_date('"SecDateTEdit"') }} > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"TreatPlanNum"', 'target': 'treatment_plan_id'},
            {'source': 'NULLIF("PatNum", 0)', 'target': 'patient_id'},
            {'source': 'NULLIF("ResponsParty", 0)', 'target': 'responsible_party_id'},
            {'source': 'NULLIF("DocNum", 0)', 'target': 'document_id'},
            {'source': 'NULLIF("SecUserNumEntry", 0)', 'target': 'entry_user_id'},
            {'source': 'NULLIF("UserNumPresenter", 0)', 'target': 'presenter_user_id'},
            {'source': 'NULLIF("MobileAppDeviceNum", 0)', 'target': 'mobile_app_device_id'}
        ]) }},
        
        -- Timestamps and dates with cleaning
        {{ clean_opendental_date('"DateTP"') }} as treatment_plan_date,
        {{ clean_opendental_date('"SecDateEntry"') }} as entry_date,
        -- Source Metadata
        {{ clean_opendental_date('"SecDateEntry"') }} as date_tentry,
        {{ clean_opendental_date('"SecDateTEdit"') }} as date_tedit,
        {{ clean_opendental_date('"DateTSigned"') }} as signed_timestamp,
        {{ clean_opendental_date('"DateTPracticeSigned"') }} as practice_signed_timestamp,
        
        -- Descriptive fields
        "Heading" as heading,
        "Note" as note,
        "Signature" as signature,
        "SignatureText" as signature_text,
        "SignaturePractice" as signature_practice,
        "SignaturePracticeText" as signature_practice_text,
        
        -- Status and type flags using boolean conversion macro
        {{ convert_opendental_boolean('"SigIsTopaz"') }} as is_signature_topaz,
        "TPStatus" as treatment_plan_status,
        "TPType" as treatment_plan_type,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"SecDateEntry"',
            updated_at_column='"SecDateTEdit"',
            created_by_column='"SecUserNumEntry"'
        ) }}
        
    from source_data
)

select * from renamed_columns
