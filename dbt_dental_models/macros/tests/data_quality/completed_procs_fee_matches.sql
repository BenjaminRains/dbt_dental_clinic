{% test completed_procs_fee_matches(model, column_name, procedure_status=none, procedure_fee=none, standard_fee=none) %}

select *
from {{ model }}
where {{ procedure_status }} = 2  -- completed procedures
  and ABS(COALESCE({{ procedure_fee }}, 0) - COALESCE({{ standard_fee }}, 0)) >= 0.01
  and {{ procedure_fee }} is not null  -- ensure we only check when fees are present

{% endtest %}