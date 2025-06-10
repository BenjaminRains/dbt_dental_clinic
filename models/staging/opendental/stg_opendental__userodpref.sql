{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'userodpref') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"UserOdPrefNum"', 'target': 'user_od_pref_id'},
            {'source': 'NULLIF("UserNum", 0)', 'target': 'user_id'},
            {'source': 'NULLIF("ClinicNum", 0)', 'target': 'clinic_id'},
            {'source': 'NULLIF("Fkey", 0)', 'target': 'fkey'}
        ]) }},
        
        -- Business Columns
        "FkeyType"::smallint as fkey_type,
        {{ clean_opendental_string('"ValueString"') }} as value_string,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
