select 
    appointment_id,
    patient_id,
    appointment_datetime,
    appointment_status,
    procedure_description,
    is_hygiene,
    entered_by_user_id,
    username
from "opendental_analytics"."public_staging"."stg_opendental__appointment" a
left join "opendental_analytics"."public_staging"."stg_opendental__userod" u 
    on a.entered_by_user_id = u.user_id
where appointment_status = 5  -- Broken/Missed
  and (procedure_description is null OR TRIM(procedure_description) = '')
  and is_hygiene = 0  -- Only look at non-hygiene appointments
