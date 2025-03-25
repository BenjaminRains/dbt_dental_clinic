select 
    appointment_id,
    patient_id,
    appointment_datetime,
    appointment_status,
    procedure_description
from {{ ref('stg_opendental__appointment') }}
where appointment_status = 5  -- Broken/Missed
  and (procedure_description is null OR TRIM(procedure_description) = '')
