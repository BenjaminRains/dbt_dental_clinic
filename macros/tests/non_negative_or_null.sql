{% test non_negative_or_null(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null 
  and {{ column_name }} < 0

{% endtest %} 