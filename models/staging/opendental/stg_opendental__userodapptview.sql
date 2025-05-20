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
        "ApptViewNum" as appt_view_id
    from Source
)

select * from Renamed
