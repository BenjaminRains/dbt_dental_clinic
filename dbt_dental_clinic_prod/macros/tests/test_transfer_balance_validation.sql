{% macro test_transfer_balance_validation(model) %}

WITH ProblematicTransfers AS (
  SELECT 
    p.payment_id
  FROM {{ ref('stg_opendental__payment') }} p
  JOIN {{ ref('stg_opendental__paysplit') }} ps
    ON p.payment_id = ps.payment_id
  WHERE p.payment_type_id = 0
  AND p.payment_notes LIKE '%INCOME TRANSFER%'
  AND ps.unearned_type IN (0, 288)
  GROUP BY p.payment_id, ABS(ps.split_amount), ps.unearned_type
  HAVING 
    (ps.unearned_type = 0 AND 
     COUNT(CASE WHEN ps.split_amount > 0 THEN 1 END) != 
     COUNT(CASE WHEN ps.split_amount < 0 THEN 1 END))
    OR
    (ps.unearned_type = 288 AND 
     COUNT(CASE WHEN ps.split_amount > 0 THEN 1 END) > 0)
)

SELECT 
  payment_allocation_id,
  payment_id,
  patient_id,
  split_amount,
  unearned_type
FROM {{ model }}
WHERE payment_id IN (SELECT payment_id FROM ProblematicTransfers)

{% endmacro %}