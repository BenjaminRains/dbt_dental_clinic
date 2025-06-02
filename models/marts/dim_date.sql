with DateSpine as (
    select date_day
    from unnest(generate_date_array('2020-01-01', '2030-12-31')) as date_day
),

Holidays as (
    select date_day as holiday_date
    from unnest([
        date '2024-01-01',  -- New Year's Day
        date '2024-01-15',  -- Martin Luther King Jr. Day
        date '2024-02-19',  -- Presidents' Day
        date '2024-05-27',  -- Memorial Day
        date '2024-06-19',  -- Juneteenth
        date '2024-07-04',  -- Independence Day
        date '2024-09-02',  -- Labor Day
        date '2024-10-14',  -- Columbus Day
        date '2024-11-11',  -- Veterans Day
        date '2024-11-28',  -- Thanksgiving Day
        date '2024-12-25'   -- Christmas Day
    ]) as date_day
),

Final as (
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
        -- Weekend/Weekday flags
        case 
            when extract(dayofweek from date_day) in (1, 7) then true 
            else false 
        end as is_weekend,
        case 
            when extract(dayofweek from date_day) between 2 and 6 then true 
            else false 
        end as is_weekday,
        -- Holiday flags
        case 
            when date_day in (select holiday_date from Holidays) then true
            else false
        end as is_holiday,
        -- Business day indicators
        case 
            when extract(dayofweek from date_day) between 2 and 6 
            and date_day not in (select holiday_date from Holidays) then true
            else false
        end as is_business_day,
        -- Business day of month
        case 
            when extract(dayofweek from date_day) between 2 and 6 
            and date_day not in (select holiday_date from Holidays) 
            then count(*) over (
                partition by extract(year from date_day), extract(month from date_day)
                order by date_day
                rows between unbounded preceding and current row
            )
            else null
        end as business_day_of_month,
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
        end as fiscal_quarter,
        -- Rolling 12 months flag
        case 
            when date_day between date_sub(current_date(), interval 12 month) and current_date() then true
            else false
        end as is_rolling_12_months,
        -- Same period prior year flag
        case 
            when date_day between date_sub(current_date(), interval 24 month) and date_sub(current_date(), interval 12 month) then true
            else false
        end as is_same_period_prior_year,
        -- Holiday adjusted flag
        case 
            when date_day in (select holiday_date from Holidays) then true
            when extract(dayofweek from date_day) in (1, 7) then true
            else false
        end as is_holiday_adjusted
    from DateSpine
)

select * from Final 