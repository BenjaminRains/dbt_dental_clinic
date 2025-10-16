{% test valid_tooth_surface(model, column_name) %}

with standard_surface_validation as (
    select *
    from {{ model }}
    where {{ column_name }} is not null 
        and {{ column_name }} != ''  -- Exclude empty strings
        and procedure_code_id not in (523, 1440)  -- Exclude odontoplasty procedures
        and not (
            {{ column_name }} ~ '^[BDFILMOURV]+$'  -- Valid surface codes:
            -- B=Buccal, D=Distal, F=Facial, I=Incisal, L=Lingual, O=Occlusal
            -- M=Mesial, U=Universal, R=Root, V=Vestibular
        )
),

odontoplasty_validation as (
    select *
    from {{ model }}
    where {{ column_name }} is not null 
        and {{ column_name }} != ''  -- Exclude empty strings
        and procedure_code_id in (523, 1440)  -- Only odontoplasty procedures
        and not (
            {{ column_name }} ~ '^[1256]$'  -- Valid numeric codes for odontoplasty:
            -- 1 = Treatment region 1
            -- 2 = Treatment region 2
            -- 5 = Treatment region 5 (multiple teeth)
            -- 6 = Treatment region 6
        )
)

select * from standard_surface_validation
union all
select * from odontoplasty_validation

{% endtest %} 