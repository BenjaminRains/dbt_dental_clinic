{% test valid_tooth_number(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null 
  and {{ column_name }} != ''  -- Exclude empty strings
  and not (
    {{ column_name }} ~ '^[A-T]$'
    or {{ column_name }} ~ '^[1-9][0-9]?$'
  )

{% endtest %} 