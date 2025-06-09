{% macro convert_opendental_boolean(column_name) %}
{#- 
    Converts OpenDental boolean fields (0/1 integers) to PostgreSQL boolean
    
    Args:
        column_name (str): The quoted column name from OpenDental (e.g., '"IsHidden"')
        
    Returns:
        SQL expression that converts 0/1 to false/true/null
        
    Example usage:
        {{ convert_opendental_boolean('"IsHidden"') }} as is_hidden,
        {{ convert_opendental_boolean('"TxtMsgOk"') }} as text_messaging_consent
-#}
    CASE 
        WHEN COALESCE({{ column_name }}, 0) = 1 THEN true
        WHEN COALESCE({{ column_name }}, 0) = 0 THEN false
        ELSE null 
    END
{% endmacro %} 