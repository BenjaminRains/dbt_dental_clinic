with date_spine as (
    select date_day
    from unnest(generate_date_array('2020-01-01', '2030-12-31')) as date_day
),

final as (
    select
        date_day as date_id,
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(dayofweek from date_day) as day_of_week,
        format_date('%B', date_day) as month_name,
        format_date('%A', date_day) as day_name,
        extract(quarter from date_day) as quarter,
        case 
            when extract(dayofweek from date_day) in (1, 7) then true 
            else false 
        end as is_weekend,
        case 
            when extract(dayofweek from date_day) between 2 and 6 then true 
            else false 
        end as is_weekday,
        -- Fiscal year (assuming fiscal year starts in October)
        case 
            when extract(month from date_day) >= 10 
            then extract(year from date_day) + 1 
            else extract(year from date_day) 
        end as fiscal_year,
        -- Fiscal quarter
        case 
            when extract(month from date_day) between 10 and 12 then 1
            when extract(month from date_day) between 1 and 3 then 2
            when extract(month from date_day) between 4 and 6 then 3
            when extract(month from date_day) between 7 and 9 then 4
        end as fiscal_quarter
    from date_spine
)

select * from final 