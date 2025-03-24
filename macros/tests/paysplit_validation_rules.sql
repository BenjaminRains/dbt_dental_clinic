{% test paysplit_validation_rules(model) %}
    {% if model.name != 'stg_opendental__paysplit' %}
        -- Skip this test for other models
        select 1 where false
    {% else %}
        with validation_errors as (
            select 
                *,
                case
                    when split_amount = 0 and is_discount_flag = false then 'Zero amount non-discount'
                    when procedure_id is null and adjustment_id is null and payplan_charge_id is null then 'Missing all references'
                    when unearned_type not in (288, 439) and unearned_type is not null then 'Invalid unearned type'
                    when unearned_type in (288, 439) and provider_id is null then 'Missing provider for unearned'
                    when unearned_type = 288 and split_amount > 25000 then 'Type 288 amount too high'
                    when unearned_type = 439 and split_amount > 16000 then 'Type 439 amount too high'
                    when paysplit_id is null or payment_id is null or split_amount is null or payment_date is null then 'Missing required field'
                    when payment_date > {{ current_date() }} then 'Future date'
                    when payment_date < '2000-01-01' then 'Date before 2000'
                    when unearned_type = 288 then 'Type 288 unearned'
                    when unearned_type = 439 then 'Type 439 unearned'
                    else 'Other issue'
                end as failure_reason
            from {{ model }}
            where split_amount = 0 and is_discount_flag = false
                or (procedure_id is null and adjustment_id is null and payplan_charge_id is null)
                or (unearned_type not in (288, 439) and unearned_type is not null)
                or (unearned_type in (288, 439) and provider_id is null)
                or (unearned_type = 288 and split_amount > 25000)
                or (unearned_type = 439 and split_amount > 16000)
                or paysplit_id is null
                or payment_id is null
                or split_amount is null
                or payment_date is null
                or payment_date > {{ current_date() }}
                or payment_date < '2000-01-01'
                or unearned_type = 288
                or unearned_type = 439
        ),
        
        -- Add summary count by validation type
        summary as (
            select 
                failure_reason,
                count(*) as count
            from validation_errors
            group by 1
            order by count desc
        )
        
        -- This will show counts in one row
        select 
            null as paysplit_id,
            null as payment_id,
            null as split_amount,
            null as payment_date,
            'SUMMARY: ' || string_agg(failure_reason || ' (' || count || ')', ', ' order by count desc) as failure_type
        from summary
        where summary.count > 0
        
        {% if execute %}
        -- Log the summary
        {% set summary_query %}
            select failure_reason, count from summary order by count desc
        {% endset %}
        {% set results = run_query(summary_query) %}
        {% do log("---- Paysplit Validation Summary ----", info=true) %}
        {% for row in results %}
            {% do log(row.failure_reason ~ ": " ~ row.count, info=true) %}
        {% endfor %}
        {% endif %}
    {% endif %}
{% endtest %} 