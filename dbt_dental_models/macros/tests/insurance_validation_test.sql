{% test insurance_validation_test(model) %}
  select
    *
  from {{ model }}
  where has_insurance_flag = 'true' 
    and procedures_with_primary_insurance = 0 
    and procedures_with_secondary_insurance = 0
{% endtest %} 