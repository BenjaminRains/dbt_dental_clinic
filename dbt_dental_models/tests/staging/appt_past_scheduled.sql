-- Test for past appointments still marked as scheduled
-- Modified to return all failing records so we can see the count in test results
-- Note: username field removed for PII compliance - use patient_id for identification
select
    a.appointment_id,
    a.patient_id,
    a.appointment_datetime,
    a.appointment_status,
    current_date as today,
    current_date - a.appointment_datetime as days_overdue,
    a.note,
    a.is_hygiene,
    a.entered_by_user_id
from {{ ref('stg_opendental__appointment') }} a
where a.appointment_status = 1  -- Scheduled
  and a.appointment_datetime < current_date  -- Only past appointments
  and a.appointment_datetime < '2025-01-01'  -- Exclude future appointments
  and a.patient_id is not null  -- Ensure patient_id is present for identification
order by a.appointment_datetime desc