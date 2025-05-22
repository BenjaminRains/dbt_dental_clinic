{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('opendental', 'employer') }}
),

entry_logs as (
    select 
        "FKey" as employer_id,
        min("EntryDateTime") as first_entry_datetime
    from {{ source('opendental', 'entrylog') }}
    where "FKeyType" = 0  -- Assuming 0 is the type for employer records
    group by "FKey"
),

renamed as (
    select
        -- Primary Key
        "EmployerNum" as employer_id,
        
        -- Attributes
        "EmpName" as employer_name,
        "Address" as address,
        "Address2" as address2,
        "City" as city,
        "State" as state,
        "Zip" as zip,
        "Phone" as phone,

        -- Required metadata columns
        current_timestamp as _loaded_at,
        el.first_entry_datetime as _created_at,
        current_timestamp as _updated_at

    from source s
    left join entry_logs el
        on s."EmployerNum" = el.employer_id
)

select * from renamed
