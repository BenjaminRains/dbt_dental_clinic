{% test warn_procedures_missing_descriptions(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is null 
  and procedure_code is not null
  and procedure_status = 2  -- Only warn for completed procedures

{% endtest %}
