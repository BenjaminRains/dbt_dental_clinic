{% test has_non_zero_balance(model, column_name) %}

select
    *
from {{ model }}
where {{ column_name }}::numeric <> 0

{% endtest %} 