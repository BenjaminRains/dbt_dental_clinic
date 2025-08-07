{% test date_complete_valid(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null 
  and {{ column_name }} < procedure_date

{% endtest %} 