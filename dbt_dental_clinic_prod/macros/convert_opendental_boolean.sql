{% macro convert_opendental_boolean(column_name) %}
{#- 
    Converts OpenDental boolean fields to PostgreSQL boolean with proper null handling
    
    OpenDental boolean fields can contain:
    - 1 (true)
    - 0 (false) 
    - "N" (defaults to false)
    - Empty strings or other non-numeric values (defaults to false)
    
    Args:
        column_name (str): The quoted column name from OpenDental (e.g., '"IsHidden"')
        
    Returns:
        SQL expression that safely converts values to true/false (defaults unknown values to false)
        
    Example usage:
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"IsProsthesis"') }} as is_prosthesis
-#}
    CASE 
        WHEN {{ column_name }}::text = '1' THEN true
        WHEN {{ column_name }}::text = '0' THEN false
        ELSE false  -- Default NULL/unknown values to false for data consistency
    END
{% endmacro %} 