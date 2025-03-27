-- This test checks that history records have logical chronology
-- If query returns rows, the test fails (showing problematic records)

WITH appointment_history AS (
  SELECT
    appointment_id,
    history_timestamp,
    ROW_NUMBER() OVER (PARTITION BY appointment_id ORDER BY history_timestamp) AS history_seq
  FROM "opendental_analytics"."public_staging"."stg_opendental__histappointment"
)

SELECT
  current.appointment_id,
  current.history_timestamp AS current_timestamp,
  previous.history_timestamp AS previous_timestamp,
  'History records out of chronological order' AS failure_reason
FROM appointment_history current
LEFT JOIN appointment_history previous 
  ON current.appointment_id = previous.appointment_id 
  AND current.history_seq = previous.history_seq + 1
WHERE 
  -- Only check for chronological ordering
  previous.history_timestamp IS NOT NULL 
  AND previous.history_timestamp > current.history_timestamp