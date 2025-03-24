{{ config(
    materialized='incremental',
    unique_key='fee_schedule_id',
    schema='staging'
) }}

with source as (
    select * 
    from {{ source('opendental', 'feesched') }}
    where "SecDateEntry" >= '2023-01-01'::date
        and "SecDateEntry" <= current_date
        and "SecDateEntry" > '2000-01-01'::date
    {% if is_incremental() %}
        and "SecDateEntry" > (select max(created_at_date) from {{ this }})
    {% endif %}
),

renamed as (
    select
        -- Primary key
        "FeeSchedNum" as fee_schedule_id,
        
        -- Attributes
        "Description" as fee_schedule_description,
        "FeeSchedType" as fee_schedule_type_id,
        "ItemOrder" as display_order,
        "IsHidden"::boolean as is_hidden,
        "IsGlobal"::smallint as is_global_flag,
        
        -- Meta fields
        "SecUserNumEntry" as created_by_user_id,
        "SecDateEntry"::date as created_at_date,
        "SecDateTEdit"::timestamp as updated_at,
        
        -- Metadata
        current_timestamp as _loaded_at

    from source
)

select * from renamed 