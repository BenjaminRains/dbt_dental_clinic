{% macro calculate_pattern_length(pattern) %}
    CASE 
        WHEN {{ pattern }} IS NULL THEN 30
        WHEN {{ pattern }} ~ '^[0-9]+$' THEN {{ pattern }}::integer
        ELSE LENGTH({{ pattern }}) * 10  -- Each character represents 10 minutes
    END
{% endmacro %} 