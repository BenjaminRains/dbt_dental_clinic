{% test suspicious_fee_amounts(model, column_name) %}

select *
from {{ model }}
where fee_id IN (217113, 219409, 218252)  -- Known decimal point errors
   or (procedure_code_id = 6 and {{ column_name }} > 1000)  -- Suspicious AOX fees

{% endtest %} 