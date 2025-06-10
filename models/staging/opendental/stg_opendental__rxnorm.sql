{{ config(
    materialized='view',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'rxnorm') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"RxNormNum"', 'target': 'rxnorm_id'}
        ]) }},
        
        -- Drug identification attributes
        {{ clean_opendental_string('"RxCui"') }} as rx_cui,
        {{ clean_opendental_string('"MmslCode"') }} as mmsl_code,
        {{ clean_opendental_string('"Description"') }} as description,
        
        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
    
    from source_data
)

select * from renamed_columns
