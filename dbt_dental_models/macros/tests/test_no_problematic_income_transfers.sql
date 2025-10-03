{% macro test_no_problematic_income_transfers(model) %}

WITH ProblematicTransfers AS (
  SELECT 
    p.payment_id
  FROM {{ ref('stg_opendental__payment') }} p
  JOIN {{ ref('stg_opendental__paysplit') }} ps
    ON p.payment_id = ps.payment_id
  WHERE p.payment_type_id = 0
  AND p.payment_notes LIKE '%INCOME TRANSFER%'
  AND (
    -- Invalid unearned_type combinations
    (ps.unearned_type = 0 AND ps.split_amount < 0) OR
    (ps.unearned_type = 288 AND ps.split_amount > 0) OR
    -- Treatment plan validation
    (ps.unearned_type = 439 AND NOT EXISTS (
      SELECT 1 
      FROM {{ ref('stg_opendental__procedurelog') }} proc
      WHERE proc.procedure_id = ps.procedure_id
      AND proc.procedure_status = 1
    ))
  )
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