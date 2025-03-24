with source as (
    select * from {{ source('opendental', 'zipcode') }}
),

renamed as (
    select
        "ZipCodeNum" as zipcode_id,
        "ZipCodeDigits" as zipcode,
        "City" as city,
        "State" as state,
        "IsFrequent" as is_frequent
    from source
)

select * from renamed 