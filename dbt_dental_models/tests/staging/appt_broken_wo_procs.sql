-- Test for broken/missed appointments without procedure descriptions
-- Note: username field removed for PII compliance - use patient_id for identification
-- Updated: Only test appointments that have linked procedures (excludes consultation-type appointments)
-- See: validation/staging/appointment/BROKEN_APPOINTMENTS_FINDINGS.md for investigation details
select 
    a.appointment_id,
    a.patient_id,
    a.appointment_datetime,
    a.appointment_status,
    a.procedure_description,
    a.is_hygiene,
    a.entered_by_user_id
from {{ ref('stg_opendental__appointment') }} a
where a.appointment_status = 5  -- Broken/Missed
  and (a.procedure_description is null OR TRIM(a.procedure_description) = '')
  and a.is_hygiene = false  -- Only look at non-hygiene appointments
  and a.patient_id is not null  -- Ensure patient_id is present for identification
  and exists (
      select 1 
      from {{ ref('stg_opendental__procedurelog') }} p
      where p.appointment_id = a.appointment_id
  )  -- Only test appointments that have linked procedures (excludes consultation appointments)
