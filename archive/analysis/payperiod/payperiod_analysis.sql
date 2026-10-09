with source as (
    select * from {{ source('opendental', 'payperiod') }}
),

renamed as (
    select
        -- Primary key
        PayPeriodNum as pay_period_id,
        
        -- Pay period dates
        DateStart as start_date,
        DateStop as end_date,
        DatePaycheck as paycheck_date
    from source
)

select * from renamed