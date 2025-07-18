-- Test for past appointments still marked as scheduled
-- Modified to return all failing records so we can see the count in test results
select
    appointment_id,
    patient_id,
    appointment_datetime,
    appointment_status,
    current_date as today,
    current_date - appointment_datetime as days_overdue,
    note,
    is_hygiene,
    entered_by_user_id,
    username
from "opendental_analytics"."staging"."stg_opendental__appointment" a
left join "opendental_analytics"."staging"."stg_opendental__userod" u 
    on a.entered_by_user_id = u.user_id
where appointment_status = 1  -- Scheduled
  and appointment_datetime < current_date  -- Only past appointments
  and appointment_datetime < '2025-01-01'  -- Exclude future appointments
order by appointment_datetime desc