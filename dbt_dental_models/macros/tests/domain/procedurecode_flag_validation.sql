{% test validate_procedure_code_flag(model, column_name, prefix, flag_column, expected_value) %}
    -- This test validates that procedures with a specific prefix have the expected flag value
    -- For example: D0 codes should have is_radiology_flag = 1
    
    select *
    from {{ model }}
    where SUBSTRING({{ column_name }}, 1, 2) = '{{ prefix }}'  -- Extract the prefix
      and {{ flag_column }} != {{ expected_value }}  -- Flag doesn't match expected value
    
{% endtest %}