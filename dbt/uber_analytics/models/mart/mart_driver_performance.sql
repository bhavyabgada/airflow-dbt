/*
================================================================================
Model: mart_driver_performance
Layer: Mart
Purpose: Driver Performance Dashboard
================================================================================

PROBLEMS SOLVED:
--------------------------------------------------------------------------------

#22 COHORT ANALYSIS
    Problem: Understanding driver behavior requires grouping by when they joined:
             - "Are 2023 drivers more engaged than 2022 drivers?"
             - "What's the 90-day retention for January cohort vs March cohort?"
             - "How does trip volume evolve over driver tenure?"
    
             Without cohort attributes, analysts must repeatedly calculate:
             - signup_month in every query
             - tenure calculations
             - Cohort comparisons become complex multi-CTEs
    
    Business Impact: Cannot measure driver acquisition ROI, hard to identify
                     which recruitment campaigns produced best drivers.
    
    Solution: PRE-CALCULATE COHORT ATTRIBUTES
    - signup_cohort: YYYY-MM format (e.g., '2024-01')
    - signup_year / signup_quarter / signup_month
    - tenure_months: How long since signup
    - tenure_bucket: 'New (<3mo)', 'Growing (3-12mo)', 'Established (1-2yr)', etc.
    
    Query becomes simple:
    ```sql
    SELECT signup_cohort, avg(trips_this_week)
    FROM mart_driver_performance
    GROUP BY signup_cohort
    ```

--------------------------------------------------------------------------------

#12 CONDITIONAL AGGREGATIONS
    Problem: Different metrics require different filtering logic:
             - "Completed trips" = trip_status = 'COMPLETED'
             - "Surge trips" = surge_multiplier > 1.0
             - "5-star trips" = rating_received = 5
             - "Tipped trips" = tips > 0
    
             Calculating all in one query without CASE statements would
             require multiple passes or complex window functions.
    
    Business Impact: Slow queries, inconsistent metric definitions.
    
    Solution: CASE-BASED CONDITIONAL AGGREGATIONS
    ```sql
    sum(case when trip_status = 'COMPLETED' then 1 else 0 end) as completed_trips,
    sum(case when surge_multiplier > 1.0 then total_fare else 0 end) as surge_revenue,
    avg(case when rating_received is not null then rating_received end) as avg_rating,
    ```
    
    All metrics in single pass, consistent definitions, reusable patterns.

--------------------------------------------------------------------------------

ENGAGEMENT STATUS:
    Classify drivers by recent activity:
    - HIGHLY_ACTIVE: 15+ trips last week
    - ACTIVE: 5-14 trips
    - LOW_ACTIVE: 1-4 trips
    - INACTIVE: 0 trips last week
    - CHURNED: No trips in 30+ days
    
    Used for driver retention targeting.

--------------------------------------------------------------------------------

PERFORMANCE TIER:
    Combine multiple metrics for overall ranking:
    - TOP_PERFORMER: Rating >= 4.8, 20+ trips/week, <10% cancellation
    - GOOD: Rating >= 4.5, 10+ trips/week
    - NEEDS_IMPROVEMENT: Rating < 4.5 or high cancellation
    - AT_RISK: Very low rating or activity

================================================================================
*/

{{ config(
    materialized='table',
    tags=['mart', 'driver', 'performance']
) }}

with trips as (
    select * from {{ ref('fct_trips') }}
),

drivers as (
    select * from {{ ref('dim_driver') }}
    where is_current = true  -- Only current version of each driver
),

-- Calculate weekly metrics per driver
weekly_metrics as (
    select
        driver_id,
        date_trunc('week', request_date)::date as week_start,
        
        -- PROBLEM #12: Conditional aggregations for different metrics
        count(*) as total_trips,
        count(case when trip_status = 'TRIP_COMPLETED' then 1 end) as completed_trips,
        count(case when trip_status like 'CANCELLED%' then 1 end) as cancelled_trips,
        count(case when surge_multiplier > 1.0 then 1 end) as surge_trips,
        count(case when tips_usd > 0 then 1 end) as tipped_trips,
        
        -- Revenue metrics
        sum(total_fare_usd) as gross_bookings_usd,
        sum(driver_earnings_usd) as driver_earnings_usd,
        sum(tips_usd) as tips_earned_usd,
        
        -- Duration & distance
        sum(duration_minutes) as total_minutes_driven,
        sum(distance_miles) as total_miles_driven,
        
        -- Quality metrics
        avg(driver_rating_received) as avg_rating_received,
        count(case when driver_rating_received = 5 then 1 end) as five_star_trips
        
    from trips
    group by 1, 2
),

-- Get latest week metrics
current_week as (
    select * from weekly_metrics
    where week_start = date_trunc('week', current_date)::date
),

-- Build driver performance mart
performance as (
    select
        d.driver_key,
        d.driver_id,
        d.city_id,
        
        -- PROBLEM #22: Cohort attributes (pre-calculated for easy analysis)
        to_char(d.signup_date, 'YYYY-MM') as signup_cohort,
        extract(year from d.signup_date)::integer as signup_year,
        extract(quarter from d.signup_date)::integer as signup_quarter,
        extract(month from d.signup_date)::integer as signup_month,
        
        -- Tenure calculation
        extract(month from age(current_date, d.signup_date))::integer as tenure_months,
        case
            when age(current_date, d.signup_date) < interval '3 months' then 'New (<3mo)'
            when age(current_date, d.signup_date) < interval '1 year' then 'Growing (3-12mo)'
            when age(current_date, d.signup_date) < interval '2 years' then 'Established (1-2yr)'
            else 'Veteran (2yr+)'
        end as tenure_bucket,
        
        -- Current week metrics
        coalesce(cw.total_trips, 0) as trips_this_week,
        coalesce(cw.completed_trips, 0) as completed_trips_this_week,
        coalesce(cw.cancelled_trips, 0) as cancelled_trips_this_week,
        coalesce(cw.surge_trips, 0) as surge_trips_this_week,
        coalesce(cw.tipped_trips, 0) as tipped_trips_this_week,
        coalesce(cw.gross_bookings_usd, 0) as gross_bookings_this_week,
        coalesce(cw.driver_earnings_usd, 0) as earnings_this_week,
        coalesce(cw.tips_earned_usd, 0) as tips_this_week,
        coalesce(cw.total_minutes_driven, 0) as minutes_driven_this_week,
        coalesce(cw.total_miles_driven, 0) as miles_driven_this_week,
        cw.avg_rating_received as avg_rating_this_week,
        coalesce(cw.five_star_trips, 0) as five_star_trips_this_week,
        
        -- Efficiency metrics
        case 
            when coalesce(cw.total_minutes_driven, 0) > 0 
            then coalesce(cw.driver_earnings_usd, 0) / (cw.total_minutes_driven / 60.0)
            else 0
        end as earnings_per_hour_this_week,
        
        -- Cancellation rate
        case 
            when coalesce(cw.total_trips, 0) > 0
            then coalesce(cw.cancelled_trips, 0)::decimal / cw.total_trips
            else 0
        end as cancellation_rate_this_week,
        
        -- Current driver rating (from dimension)
        d.current_rating,
        d.driver_status,
        d.is_active,
        
        -- ENGAGEMENT STATUS: Classify by recent activity
        case
            when coalesce(cw.completed_trips, 0) >= 15 then 'HIGHLY_ACTIVE'
            when coalesce(cw.completed_trips, 0) >= 5 then 'ACTIVE'
            when coalesce(cw.completed_trips, 0) >= 1 then 'LOW_ACTIVE'
            when d.last_active_date >= current_date - 30 then 'INACTIVE'
            else 'CHURNED'
        end as engagement_status,
        
        -- PERFORMANCE TIER: Overall driver ranking
        case
            when d.current_rating >= 4.8 
                 and coalesce(cw.completed_trips, 0) >= 20
                 and (coalesce(cw.cancelled_trips, 0)::decimal / nullif(cw.total_trips, 0)) < 0.1
            then 'TOP_PERFORMER'
            when d.current_rating >= 4.5 and coalesce(cw.completed_trips, 0) >= 10
            then 'GOOD'
            when d.current_rating < 4.5 
                 or (coalesce(cw.cancelled_trips, 0)::decimal / nullif(cw.total_trips, 0)) > 0.2
            then 'NEEDS_IMPROVEMENT'
            when coalesce(cw.completed_trips, 0) = 0
            then 'AT_RISK'
            else 'STANDARD'
        end as performance_tier,
        
        -- Audit
        current_timestamp as _mart_loaded_at,
        '{{ invocation_id }}' as _invocation_id
        
    from drivers d
    left join current_week cw on d.driver_id = cw.driver_id
)

select * from performance
