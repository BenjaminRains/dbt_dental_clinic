{{ config(
    materialized='table',
    schema='staging'
) }}

with source_data as (
    select * from {{ source('opendental', 'pharmacy') }}
),

renamed_columns as (
    select
        -- Primary and Foreign Key transformations using macro
        {{ transform_id_columns([
            {'source': '"PharmacyNum"', 'target': 'pharmacy_id'}
        ]) }},
        
        -- Identifiers
        {{ clean_opendental_string('"PharmID"') }} as pharm_id,
        
        -- Business attributes
        {{ clean_opendental_string('"StoreName"') }} as store_name,
        {{ clean_opendental_string('"Phone"') }} as phone,
        {{ clean_opendental_string('"Fax"') }} as fax,
        
        -- Address information
        {{ clean_opendental_string('"Address"') }} as address,
        {{ clean_opendental_string('"Address2"') }} as address2,
        {{ clean_opendental_string('"City"') }} as city,
        {{ clean_opendental_string('"State"') }} as state,
        {{ clean_opendental_string('"Zip"') }} as zip,
        
        -- Additional information
        {{ clean_opendental_string('"Note"') }} as note,
        
        -- Timestamps
        "DateTStamp" as date_tstamp,

        -- Standardized metadata using macro
        {{ standardize_metadata_columns(
            created_at_column='"DateTStamp"',
            updated_at_column='"DateTStamp"',
            created_by_column=none
        ) }}
        
    from source_data
)

select * from renamed_columns
