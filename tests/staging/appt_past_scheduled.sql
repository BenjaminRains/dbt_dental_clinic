-- Test for past appointments still marked as scheduled
-- Modified to return all failing records so we can see the count in test results
select
    appointment_id,
    patient_id,
    appointment_datetime,
    appointment_status,
    current_date as today,
    current_date - appointment_datetime as days_overdue
from {{ ref('stg_opendental__appointment') }}
where appointment_status = 1  -- Scheduled
  and appointment_datetime < current_date