select 
    a.appointment_id,
    a.patient_id,
    a.appointment_datetime,
    a.appointment_status,
    a.procedure_description,
    a.is_hygiene,
    a.entered_by_user_id,
    u.username
from {{ ref('stg_opendental__appointment') }} a
left join {{ ref('stg_opendental__userod') }} u 
    on a.entered_by_user_id = u.user_id
where a.appointment_status = 5  -- Broken/Missed
  and (a.procedure_description is null OR TRIM(a.procedure_description) = '')
  and a.is_hygiene = false  -- Only look at non-hygiene appointments
