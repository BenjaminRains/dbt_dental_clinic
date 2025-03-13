with source as (
    select * from {{ source('opendental', 'zipcode') }}
),

renamed as (
    select
        -- Primary key
        ZipCodeNum as zipcode_id,
        
        -- Location details
        ZipCodeDigits as zipcode,
        City as city_name,
        State as state_code,
        
        -- Usage indicator
        IsFrequent as is_frequent_flag
    from source
)

select * from renamed