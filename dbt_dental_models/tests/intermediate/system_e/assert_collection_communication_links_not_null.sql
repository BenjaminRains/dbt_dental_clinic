-- Failing rows: collection communications missing commlog_id or patient_id
SELECT
    collection_communication_id,
    commlog_id,
    patient_id,
    collection_task_id
FROM {{ ref('int_collection_communication') }}
WHERE
    commlog_id IS NULL
    OR patient_id IS NULL
