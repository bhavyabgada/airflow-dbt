/*
    Model: dim_date
    Layer: Dimension
    Description: 
        Date dimension with calendar and fiscal attributes.
        Role-playing dimension - used for multiple date columns in facts.
    
    Problems Addressed:
    - #8 Fiscal vs Calendar: Both calendar and fiscal attributes
    - #20 Role-Playing Dimensions: Same dim for request/pickup/dropoff dates
*/

{{ config(
    materialized='table',
    tags=['dimension', 'date']
) }}

with date_spine as (
    -- Generate dates from 2020 to 2030
    select 
        generate_series(
            '2020-01-01'::date,
            '2030-12-31'::date,
            '1 day'::interval
        )::date as date_day
),

calendar_attributes as (
    select
        -- Primary key
        to_char(date_day, 'YYYYMMDD')::integer as date_key,
        date_day as full_date,
        
        -- Calendar attributes
        extract(year from date_day)::integer as calendar_year,
        extract(quarter from date_day)::integer as calendar_quarter,
        extract(month from date_day)::integer as calendar_month,
        extract(week from date_day)::integer as calendar_week,
        extract(day from date_day)::integer as day_of_month,
        extract(dow from date_day)::integer as day_of_week,  -- 0=Sunday
        extract(doy from date_day)::integer as day_of_year,
        
        -- Names
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Mon') as month_short_name,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Dy') as day_short_name,
        
        -- Quarters
        'Q' || extract(quarter from date_day)::varchar as quarter_name,
        extract(year from date_day)::varchar || '-Q' || extract(quarter from date_day)::varchar as year_quarter,
        
        -- ISO week (Mon-Sun) for payout periods
        extract(isoyear from date_day)::integer as iso_year,
        extract(week from date_day)::integer as iso_week,
        
        -- Week boundaries (for driver payouts - Mon to Sun)
        date_trunc('week', date_day)::date as week_start_date,
        (date_trunc('week', date_day) + interval '6 days')::date as week_end_date,
        
        -- Month boundaries
        date_trunc('month', date_day)::date as month_start_date,
        (date_trunc('month', date_day) + interval '1 month - 1 day')::date as month_end_date,
        
        -- Quarter boundaries
        date_trunc('quarter', date_day)::date as quarter_start_date,
        (date_trunc('quarter', date_day) + interval '3 months - 1 day')::date as quarter_end_date,
        
        -- Flags
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend,
        case when extract(dow from date_day) = 1 then true else false end as is_monday,
        case when extract(dow from date_day) = 5 then true else false end as is_friday,
        
        -- Relative dates (useful for reporting)
        date_day = current_date as is_today,
        date_day = current_date - 1 as is_yesterday,
        date_day >= date_trunc('week', current_date) and date_day <= current_date as is_current_week,
        date_day >= date_trunc('month', current_date) and date_day <= current_date as is_current_month
        
    from date_spine
),

-- Add fiscal calendar for different regions
with_fiscal as (
    select
        c.*,
        
        -- US Fiscal (Jan-Dec, same as calendar)
        c.calendar_year as fiscal_year_us,
        c.calendar_quarter as fiscal_quarter_us,
        
        -- UK/Japan Fiscal (Apr-Mar)
        case 
            when c.calendar_month >= 4 then c.calendar_year + 1
            else c.calendar_year
        end as fiscal_year_uk,
        case 
            when c.calendar_month >= 4 then ((c.calendar_month - 4) / 3 + 1)::integer
            else ((c.calendar_month + 8) / 3 + 1)::integer
        end as fiscal_quarter_uk,
        
        -- Australia Fiscal (Jul-Jun)
        case 
            when c.calendar_month >= 7 then c.calendar_year + 1
            else c.calendar_year
        end as fiscal_year_au,
        case 
            when c.calendar_month >= 7 then ((c.calendar_month - 7) / 3 + 1)::integer
            else ((c.calendar_month + 5) / 3 + 1)::integer
        end as fiscal_quarter_au
        
    from calendar_attributes c
)

select 
    *,
    -- Audit
    current_timestamp as _loaded_at
from with_fiscal

