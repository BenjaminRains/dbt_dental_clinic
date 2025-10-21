{% test histappointment_chronology(model) %}

WITH ordered_history AS (
  SELECT
    appointment_id,
    history_timestamp,
    LAG(history_timestamp) OVER (
      PARTITION BY appointment_id 
      ORDER BY history_timestamp
    ) as prev_timestamp
  FROM {{ model }}
)

SELECT 
  appointment_id,
  history_timestamp,
  prev_timestamp
FROM ordered_history
WHERE prev_timestamp IS NOT NULL  -- Skip first record of each appointment
  AND history_timestamp < prev_timestamp  -- Check for out-of-order records

{% endtest %} 