{% macro test_insurance_verification(
    model,
    column_name,
    verify_column,
    verify_type,
    severity='WARN'
) %}

with base_records as (
    select 
        {{ column_name }},
        effective_date,
        termination_date,
        insurance_plan_id,
        case 
            when date_part('month', effective_date) = 1 then 'January'
            else 'Non-January'
        end as record_type
    from {{ model }}
    where {{ column_name }} is not null 
        and effective_date is not null 
        and effective_date >= '2020-01-01'::date
        and effective_date <= '{{ var("max_valid_date") }}'::date
        and (termination_date is null or termination_date >= current_date)
        and insurance_plan_id not in (
            select insurance_plan_id 
            from {{ ref('stg_opendental__insplan') }}
            where hide_from_verify_list = true
        )
        and (
            -- For January records (new year insurance changes)
            (date_part('month', effective_date) = 1 
             and effective_date <= current_date
             and effective_date >= current_date - interval '365 days'
             and effective_date <= current_date - interval '3 days'
             and insurance_plan_id in (
                 select insurance_plan_id
                 from {{ model }}
                 group by insurance_plan_id
                 having count(*) filter (where date_part('month', effective_date) = 1) / count(*) >= 0.7
             ))
            or
            -- For non-January records (standard verification)
            (date_part('month', effective_date) != 1 
             and effective_date <= current_date
             and effective_date >= current_date - interval '180 days'
             and insurance_plan_id in (
                 select insurance_plan_id
                 from {{ model }}
                 group by insurance_plan_id
                 having count(*) filter (where date_part('month', effective_date) = 1) / count(*) < 0.7
             ))
        )
),
verification_check as (
    select 
        b.{{ column_name }},
        b.record_type,
        b.effective_date,
        case when v1.{{ verify_column }} is not null or v2.{{ verify_column }} is not null then true else false end as has_verification
    from base_records b
    left join {{ ref('stg_opendental__insverify') }} v1
        on b.{{ column_name }} = v1.{{ verify_column }}
        and v1.verify_type = {{ verify_type }}
    left join {{ ref('stg_opendental__insverifyhist') }} v2
        on b.{{ column_name }} = v2.{{ verify_column }}
        and v2.verify_type = {{ verify_type }}
),
verification_summary as (
    select 
        record_type,
        count(*) as total_records,
        count(*) filter (where has_verification) as records_with_verification,
        count(*) filter (where not has_verification) as missing_verifications,
        round((count(*) filter (where not has_verification))::numeric / count(*) * 100, 1) as missing_percentage
    from verification_check
    group by record_type
)
select *
from verification_summary
where missing_percentage > 50

{% endmacro %} 