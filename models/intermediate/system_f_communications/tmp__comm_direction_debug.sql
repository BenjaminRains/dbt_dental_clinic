{{ config(
    materialized='table',
    schema='intermediate'
) }}

WITH BaseCommunications AS (
    SELECT
        base.communication_id,
        base.patient_id,
        base.direction  -- Only testing this column
    FROM {{ ref('int_patient_communications_base') }} base
),

FilteredCommunications AS (
    SELECT *
    FROM BaseCommunications
    WHERE direction = 'outbound'
)

SELECT *
FROM FilteredCommunications
LIMIT 10 