-- Test to track missing appointments and their history records
-- This test helps monitor the expected pattern of deleted appointments with preserved history

with missing_appointments as (
    select 
        h.appointment_id,
        count(*) as history_record_count
    from {{ ref('stg_opendental__histappointment') }} h
    left join {{ ref('stg_opendental__appointment') }} a
        on h.appointment_id = a.appointment_id
    where a.appointment_id is null
    group by h.appointment_id
),

metrics as (
    select
        count(*) as total_missing_appointments,
        sum(history_record_count) as total_history_records,
        round(avg(history_record_count), 2) as avg_history_records_per_appointment,
        count(case when history_record_count = 1 then 1 end) as appointments_with_one_record,
        count(case when history_record_count between 2 and 5 then 1 end) as appointments_with_2_to_5_records,
        count(case when history_record_count > 5 then 1 end) as appointments_with_more_than_5_records
    from missing_appointments
)

select *
from metrics
where 
    -- Alert if the number of missing appointments changes significantly
    total_missing_appointments not between 3000 and 3500
    -- Alert if the average history records per appointment changes significantly
    or avg_history_records_per_appointment not between 4.0 and 4.5
    -- Alert if the distribution of history records changes significantly
    or appointments_with_one_record not between 500 and 700
    or appointments_with_2_to_5_records not between 1600 and 1900
    or appointments_with_more_than_5_records not between 700 and 800 