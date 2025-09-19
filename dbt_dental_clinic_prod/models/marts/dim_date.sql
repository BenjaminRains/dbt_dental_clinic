{{
    config(
        materialized='table',
        schema='marts',
        unique_key='date_id',
        on_schema_change='fail',
        indexes=[
            {'columns': ['date_id'], 'unique': true},
            {'columns': ['date_day']},
            {'columns': ['year', 'month']},
            {'columns': ['fiscal_year', 'fiscal_quarter']},
            {'columns': ['is_business_day']}
        ]
    )
}}

/*
    Dimension model for Date
    Part of System A: Core Data Foundation
    
    This model:
    1. Provides comprehensive time intelligence and calendar attributes
    2. Supports fiscal period alignment for financial reporting
    3. Enables business day calculations for operational metrics
    4. Facilitates holiday-adjusted performance analysis
    
    Business Logic Features:
    - Calendar attributes: year, month, day, quarter, day of week
    - Fiscal periods: fiscal year (Oct-Sep) and fiscal quarters
    - Business days: weekdays excluding US federal holidays
    - Time intelligence: rolling periods and prior year comparisons
    
    Key Metrics:
    - Business day calculations for operational efficiency
    - Holiday-adjusted performance indicators
    - Fiscal period alignment for financial reporting
    - Rolling period analysis for trend identification
    
    Data Quality Notes:
    - Date range: 2020-01-01 through 2030-12-31 (11 years)
    - Holiday calendar includes major US federal holidays
    - Business day logic excludes weekends and holidays
    
    Performance Considerations:
    - Pre-calculated flags minimize runtime calculations
    - Indexed on date_id for optimal join performance
    - Static nature allows for materialized table optimization
    
    Dependencies:
    - None (self-contained date generation using PostgreSQL generate_series)
*/

with source_date as (
    select date_day::date as date_day
    from generate_series(
        '2020-01-01'::date,
        '2030-12-31'::date,
        '1 day'::interval
    ) as date_day
),

date_holidays as (
    select date_day as holiday_date
    from source_date
    where 
        -- New Year's Day (January 1)
        (extract(month from date_day) = 1 and extract(day from date_day) = 1)
        -- Martin Luther King Jr. Day (3rd Monday in January)
        or (extract(month from date_day) = 1 
            and extract(dow from date_day) = 1  -- Monday (1 in PostgreSQL)
            and extract(day from date_day) between 15 and 21)
        -- Presidents' Day (3rd Monday in February)
        or (extract(month from date_day) = 2 
            and extract(dow from date_day) = 1  -- Monday
            and extract(day from date_day) between 15 and 21)
        -- Memorial Day (Last Monday in May)
        or (extract(month from date_day) = 5 
            and extract(dow from date_day) = 1  -- Monday
            and extract(day from date_day) >= 25)
        -- Juneteenth (June 19)
        or (extract(month from date_day) = 6 and extract(day from date_day) = 19)
        -- Independence Day (July 4)
        or (extract(month from date_day) = 7 and extract(day from date_day) = 4)
        -- Labor Day (First Monday in September)
        or (extract(month from date_day) = 9 
            and extract(dow from date_day) = 1  -- Monday
            and extract(day from date_day) <= 7)
        -- Columbus Day (Second Monday in October)
        or (extract(month from date_day) = 10 
            and extract(dow from date_day) = 1  -- Monday
            and extract(day from date_day) between 8 and 14)
        -- Veterans Day (November 11)
        or (extract(month from date_day) = 11 and extract(day from date_day) = 11)
        -- Thanksgiving Day (4th Thursday in November)
        or (extract(month from date_day) = 11 
            and extract(dow from date_day) = 4  -- Thursday (4 in PostgreSQL)
            and extract(day from date_day) between 22 and 28)
        -- Christmas Day (December 25)
        or (extract(month from date_day) = 12 and extract(day from date_day) = 25)
),

date_enhanced as (
    select
        -- Primary identification
        date_day as date_id,
        date_day,
        
        -- Calendar attributes
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(dow from date_day) as day_of_week,
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Day') as day_name,
        extract(quarter from date_day) as quarter,
        
        -- Weekend/Weekday flags
        case 
            when extract(dow from date_day) in (0, 6) then true 
            else false 
        end as is_weekend,
        case 
            when extract(dow from date_day) between 1 and 5 then true 
            else false 
        end as is_weekday,
        
        -- Holiday flags
        case 
            when date_day in (select holiday_date from date_holidays) then true
            else false
        end as is_holiday,
        
        -- Business day indicators
        case 
            when extract(dow from date_day) between 1 and 5 
            and date_day not in (select holiday_date from date_holidays) then true
            else false
        end as is_business_day,
        
        -- Business day of month
        case 
            when extract(dow from date_day) between 1 and 5 
            and date_day not in (select holiday_date from date_holidays) 
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
            when date_day between (current_date - interval '12 months') and current_date then true
            else false
        end as is_rolling_12_months,
        
        -- Same period prior year flag
        case 
            when date_day between (current_date - interval '24 months') and (current_date - interval '12 months') then true
            else false
        end as is_same_period_prior_year,
        
        -- Holiday adjusted flag
        case 
            when date_day in (select holiday_date from date_holidays) then true
            when extract(dow from date_day) in (0, 6) then true
            else false
        end as is_holiday_adjusted,
        
        -- Metadata
        current_timestamp as _loaded_at,
        current_timestamp as _created_at,
        current_timestamp as _updated_at,
        current_timestamp as _transformed_at,
        current_timestamp as _mart_refreshed_at
    from source_date
),

final as (
    select * from date_enhanced
)

select * from final 
