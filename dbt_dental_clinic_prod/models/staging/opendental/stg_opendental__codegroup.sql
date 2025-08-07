{{ config(
    materialized='view'
) }}

with source_data as (
    select * from {{ source('opendental', 'codegroup') }}
),

renamed_columns as (
    select
        -- Primary Key
        {{ transform_id_columns([
            {'source': '"CodeGroupNum"', 'target': 'codegroup_id'}
        ]) }},
        
        -- Attributes
        "GroupName" as group_name,
        "ProcCodes" as proc_codes,
        "ItemOrder" as item_order,
        
        -- Boolean Fields
        {{ convert_opendental_boolean('"CodeGroupFixed"') }} as is_fixed,
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"ShowInAgeLimit"') }} as show_in_age_limit,
        
        -- Metadata columns
        {{ standardize_metadata_columns(
            created_at_column=none,
            updated_at_column=none,
            created_by_column=none
        ) }}
    
    from source_data
)

select * from renamed_columns
