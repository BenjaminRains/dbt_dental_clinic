with source as (
    select * from {{ source('opendental', 'fee') }}
),

renamed as (
    select
        -- Primary Key
        "FeeNum"::integer as fee_id,
        
        -- Foreign Keys
        "FeeSched"::bigint as fee_schedule_id,
        "CodeNum"::bigint as procedure_code_id,
        "ClinicNum" as clinic_id,
        "ProvNum"::bigint as provider_id,
        "SecUserNumEntry" as user_entry_id,
        
        -- Regular Fields
        "Amount"::double precision as fee_amount,
        "OldCode" as ada_code,
        
        -- Smallint Fields (keeping as smallint)
        "UseDefaultFee"::smallint as is_default_fee,
        "UseDefaultCov"::smallint as is_default_coverage,
        
        -- Dates and Timestamps
        "SecDateEntry"::date as created_at,
        "SecDateTEdit"::timestamp as updated_at
        
    from source
)

select * from renamed
