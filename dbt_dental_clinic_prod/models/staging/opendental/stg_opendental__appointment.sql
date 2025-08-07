-- Temporarily simplified for debugging
select 
    "AptNum" as appointment_id,
    "PatNum" as patient_id,
    "AptDateTime" as appointment_datetime,
    "AptStatus" as appointment_status,
    "Note" as note,
    current_timestamp as _loaded_at
from {{ source('opendental', 'appointment') }}
where "AptDateTime" >= '2023-01-01'::timestamp
