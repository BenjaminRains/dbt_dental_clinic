{{ config(
    materialized='view'
) }}

with source_data as (
    select * 
    from {{ source('opendental', 'feesched') }}
    where "SecDateEntry" >= '2023-01-01'::date
        and "SecDateEntry" <= current_date
        and "SecDateEntry" > '2000-01-01'::date
),

renamed_columns as (
    select
        -- Primary key and IDs
        {{ transform_id_columns([
            {'source': '"FeeSchedNum"', 'target': 'fee_schedule_id'},
            {'source': '"FeeSchedType"', 'target': 'fee_schedule_type_id'}
        ]) }},
        
        -- Attributes
        "Description" as fee_schedule_description,
        "ItemOrder" as display_order,
        
        -- Boolean fields
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"IsGlobal"') }} as is_global_flag,
        
        -- Date fields
        {{ clean_opendental_date('"SecDateEntry"') }} as date_created,
        {{ clean_opendental_date('"SecDateTEdit"') }} as date_updated,
        
        -- Standardized metadata columns
        {{ standardize_metadata_columns() }},
        "SecUserNumEntry" as sec_user_num_entry

    from source_data
)

select * from renamed_columns 
