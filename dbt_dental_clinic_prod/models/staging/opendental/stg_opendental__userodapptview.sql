{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'userodapptview') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"UserodApptViewNum"', 'target': 'userod_appt_view_id'},
            {'source': 'NULLIF("UserNum", 0)', 'target': 'user_id'},
            {'source': 'NULLIF("ClinicNum", 0)', 'target': 'clinic_id'},
            {'source': 'NULLIF("ApptViewNum", 0)', 'target': 'appt_view_id'}
        ]) }},
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}

    from source_data
)

select * from renamed_columns
