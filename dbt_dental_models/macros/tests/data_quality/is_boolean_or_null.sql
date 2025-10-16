{% test is_boolean_or_null(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null 
  and {{ column_name }} not in (0, 1)

{% endtest %} 