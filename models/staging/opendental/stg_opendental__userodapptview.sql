{{ config(
    materialized='view'
) }}

with Source as (
    select * from {{ source('opendental', 'userodapptview') }}
),

Renamed as (
    select
        "UserodApptViewNum" as userod_appt_view_id,
        "UserNum" as user_id,
        "ClinicNum" as clinic_id,
        "ApptViewNum" as appt_view_id,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at
    from Source
)

select * from Renamed
