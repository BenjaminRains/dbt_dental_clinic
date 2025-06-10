{% macro convert_opendental_boolean(column_name) %}
{#- 
    Converts OpenDental boolean fields (0/1 integers) to PostgreSQL boolean
    Note: For columns that are already boolean in PostgreSQL, use them directly
    
    Args:
        column_name (str): The quoted column name from OpenDental (e.g., '"IsHidden"')
        
    Returns:
        SQL expression that converts 0/1 integers to false/true/null
        
    Example usage:
        {{ convert_opendental_boolean('"PasswordIsStrong"') }} as has_strong_password,
        "IsHidden"::boolean as is_hidden  -- For columns already boolean
-#}
    CASE 
        WHEN COALESCE({{ column_name }}::integer, 0) = 1 THEN true
        WHEN COALESCE({{ column_name }}::integer, 0) = 0 THEN false
        ELSE null 
    END
{% endmacro %} 