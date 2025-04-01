{{ config(
    materialized='incremental',
    unique_key='fee_id',
    schema='staging'
) }}

with source as (
    select * 
    from {{ source('opendental', 'fee') }}
    where "SecDateEntry" >= '2023-01-01'::date
        and "SecDateEntry" <= current_date
        and "SecDateEntry" > '2000-01-01'::date
    {% if is_incremental() %}
        and "SecDateEntry" > (select max(created_at) from {{ this }})
    {% endif %}
),

FeeStats as (
    select 
        "CodeNum" as procedure_code_id,
        avg("Amount") as avg_fee_amount,
        stddev("Amount") as fee_stddev,
        count(*) as fee_count
    from {{ source('opendental', 'fee') }}
    group by "CodeNum"
),

ValidFeeSchedules as (
    select distinct fee_schedule_id
    from {{ ref('stg_opendental__feesched') }}
),

renamed as (
    select
        -- Primary Key
        "FeeNum"::integer as fee_id,
        
        -- Foreign Keys
        "FeeSched"::bigint as fee_schedule_id,
        "CodeNum"::bigint as procedure_code_id,
        "ClinicNum" as clinic_id,
        "ProvNum"::bigint as provider_id,  -- Source field is ProvNum, nullable
        "SecUserNumEntry" as user_entry_id,
        
        -- Regular Fields
        "Amount"::double precision as fee_amount,
        "OldCode" as ada_code,
        
        -- Fee Statistics
        fs.avg_fee_amount,
        fs.fee_stddev,
        fs.fee_count,
        
        -- Smallint Fields
        "UseDefaultFee"::smallint as is_default_fee,
        "UseDefaultCov"::smallint as is_default_coverage,
        
        -- Dates and Timestamps
        "SecDateEntry"::date as created_at,
        "SecDateTEdit"::timestamp as updated_at,
        
        -- Missing Fee Schedule Flag
        case 
            when vfs.fee_schedule_id is null then true 
            else false 
        end as has_missing_fee_schedule,
        
        -- Metadata
        current_timestamp as _loaded_at
        
    from source
    left join FeeStats fs on fs.procedure_code_id = source."CodeNum"
    left join ValidFeeSchedules vfs on vfs.fee_schedule_id = source."FeeSched"
)

select * from renamed