{% test payment_validation_rules(model) %}
    with validation_errors as (
        select *
        from {{ model }}
        where 1=0  -- Start with no errors
        
        -- Payment amount validation
        or payment_amount <= 0  -- Payments should be positive
        
        -- Payment type validation
        or payment_type_id not in (0, 69, 70, 71, 72, 391, 412, 417, 574, 634)
        
        -- Date validation
        or payment_date > {{ current_date() }}
        or payment_date < '2000-01-01'
        
        -- Required fields validation
        or payment_id is null
        or patient_id is null
        or payment_amount is null
        or payment_date is null
        or payment_type_id is null
    )
    
    select * from validation_errors
{% endtest %} 