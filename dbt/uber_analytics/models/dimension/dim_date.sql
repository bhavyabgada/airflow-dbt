/*
================================================================================
Model: dim_date
Layer: Dimension
Type: Role-Playing Dimension
================================================================================

PROBLEMS SOLVED:
--------------------------------------------------------------------------------

#8 FISCAL VS CALENDAR YEAR
    Problem: Different regions use different fiscal year calendars:
             - US/LATAM: January - December (calendar year)
             - UK/Japan: April - March (fiscal year)
             - Australia: July - June (fiscal year)
    
             A trip on March 15, 2024 is:
             - US Fiscal: FY2024 Q1
             - UK Fiscal: FY2024 Q4 (year ending March)
             - AU Fiscal: FY2024 Q3
    
    Business Impact: Wrong fiscal reporting, incorrect quarter comparisons.
    
    Solution:
    - Include MULTIPLE fiscal calendar columns for each region
    - fiscal_year_us, fiscal_quarter_us (Jan-Dec)
    - fiscal_year_uk, fiscal_quarter_uk (Apr-Mar)
    - fiscal_year_au, fiscal_quarter_au (Jul-Jun)
    - Reports can use appropriate fiscal columns based on region

--------------------------------------------------------------------------------

#17 ROLE-PLAYING DIMENSIONS
    Problem: A single trip has multiple meaningful dates:
             - request_date: When customer ordered the ride
             - pickup_date: When trip actually started
             - dropoff_date: When trip ended
             - payment_date: When payment was processed
    
             Each needs different date dimension attributes:
             - "Daily requests by day of week" (request_date)
             - "Average trip duration by pickup hour" (pickup_date)
    
    Business Impact: Need separate date dimensions? No - inefficient!
    
    Solution: ROLE-PLAYING DIMENSION
    - Build ONE comprehensive date dimension
    - Fact table has MULTIPLE foreign keys to same dimension:
      - request_date_key → dim_date
      - pickup_date_key → dim_date
      - dropoff_date_key → dim_date
    - BI tools alias the same table for different date contexts
    
    Fact table pattern:
    ```sql
    to_char(request_timestamp::date, 'YYYYMMDD')::integer as request_date_key,
    to_char(pickup_timestamp::date, 'YYYYMMDD')::integer as pickup_date_key,
    to_char(dropoff_timestamp::date, 'YYYYMMDD')::integer as dropoff_date_key,
    ```

--------------------------------------------------------------------------------

PAYOUT WEEK CALCULATION:
    Uber pays drivers weekly (Monday to Sunday).
    - week_start_date: Monday of the week
    - week_end_date: Sunday of the week
    - iso_week: Week number (1-52)
    
    Used by fct_driver_earnings for weekly payout calculations.

================================================================================
*/

{{ config(
    materialized='table',
    tags=['dimension', 'date']
) }}

-- Generate dates from 2020 to 2030 (11 years)
with date_spine as (
    select 
        generate_series(
            '2020-01-01'::date,
            '2030-12-31'::date,
            '1 day'::interval
        )::date as date_day
),

-- Add calendar attributes
calendar_attributes as (
    select
        -- Primary key: Integer in YYYYMMDD format (efficient for joins)
        to_char(date_day, 'YYYYMMDD')::integer as date_key,
        date_day as full_date,
        
        -- Calendar hierarchy
        extract(year from date_day)::integer as calendar_year,
        extract(quarter from date_day)::integer as calendar_quarter,
        extract(month from date_day)::integer as calendar_month,
        extract(week from date_day)::integer as calendar_week,
        extract(day from date_day)::integer as day_of_month,
        extract(dow from date_day)::integer as day_of_week,  -- 0=Sunday
        extract(doy from date_day)::integer as day_of_year,
        
        -- Human-readable names
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Mon') as month_short_name,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Dy') as day_short_name,
        
        -- Quarter labels
        'Q' || extract(quarter from date_day)::varchar as quarter_name,
        extract(year from date_day)::varchar || '-Q' || extract(quarter from date_day)::varchar as year_quarter,
        
        -- ISO week (Monday-Sunday) - used for driver payout periods
        extract(isoyear from date_day)::integer as iso_year,
        extract(week from date_day)::integer as iso_week,
        
        -- Payout week boundaries (Mon-Sun)
        date_trunc('week', date_day)::date as week_start_date,
        (date_trunc('week', date_day) + interval '6 days')::date as week_end_date,
        
        -- Month boundaries
        date_trunc('month', date_day)::date as month_start_date,
        (date_trunc('month', date_day) + interval '1 month - 1 day')::date as month_end_date,
        
        -- Quarter boundaries
        date_trunc('quarter', date_day)::date as quarter_start_date,
        (date_trunc('quarter', date_day) + interval '3 months - 1 day')::date as quarter_end_date,
        
        -- Useful flags
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend,
        case when extract(dow from date_day) = 1 then true else false end as is_monday,
        case when extract(dow from date_day) = 5 then true else false end as is_friday,
        
        -- Relative date flags (useful for default filters in BI tools)
        date_day = current_date as is_today,
        date_day = current_date - 1 as is_yesterday,
        date_day >= date_trunc('week', current_date) and date_day <= current_date as is_current_week,
        date_day >= date_trunc('month', current_date) and date_day <= current_date as is_current_month
        
    from date_spine
),

-- PROBLEM #8: Add fiscal calendar for different regions
with_fiscal as (
    select
        c.*,
        
        -- US Fiscal Year (January - December, same as calendar)
        c.calendar_year as fiscal_year_us,
        c.calendar_quarter as fiscal_quarter_us,
        
        -- UK/Japan Fiscal Year (April - March)
        -- March 2024 = FY2024, April 2024 = FY2025
        case 
            when c.calendar_month >= 4 then c.calendar_year + 1
            else c.calendar_year
        end as fiscal_year_uk,
        case 
            when c.calendar_month >= 4 then ((c.calendar_month - 4) / 3 + 1)::integer
            else ((c.calendar_month + 8) / 3 + 1)::integer
        end as fiscal_quarter_uk,
        
        -- Australia Fiscal Year (July - June)
        -- June 2024 = FY2024, July 2024 = FY2025
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
