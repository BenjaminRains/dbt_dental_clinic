{% test paysplit_validation_rules(model) %}
    with validation_errors as (
        select *
        from {{ model }}
        where 1=0  -- Start with no errors
        
        -- Split amount validation
        or (split_amount = 0 and is_discount_flag = false)  -- Amount should not be 0 unless it's a discount
        
        -- Reference validation
        or (procedure_id is null 
            and adjustment_id is null 
            and payplan_charge_id is null)  -- Must have at least one reference
        
        -- Unearned type validation
        or (unearned_type not in (288, 439) and unearned_type is not null)  -- Valid unearned types
        or (unearned_type in (288, 439) and provider_id is null)  -- Unearned must have provider
        
        -- Amount threshold validation
        or (unearned_type = 288 and split_amount > 25000)  -- Type 288 limit
        or (unearned_type = 439 and split_amount > 16000)  -- Type 439 limit
        
        -- Required fields validation
        or paysplit_id is null
        or payment_id is null
        or split_amount is null
        or payment_date is null
        
        -- Date validation
        or payment_date > {{ current_date() }}
        or payment_date < '2000-01-01'
    )
    
    select * from validation_errors
{% endtest %} 