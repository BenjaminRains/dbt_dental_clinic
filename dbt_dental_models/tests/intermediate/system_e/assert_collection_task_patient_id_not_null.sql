-- Failing rows: collection tasks missing patient_id
SELECT
    collection_task_id,
    campaign_id,
    patient_id
FROM {{ ref('int_collection_tasks') }}
WHERE patient_id IS NULL
