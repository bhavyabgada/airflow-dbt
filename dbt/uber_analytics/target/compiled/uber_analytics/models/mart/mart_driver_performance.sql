/*
    Model: mart_driver_performance
    Layer: Mart
    Description: 
        Driver performance metrics including earnings, ratings, and utilization.
        Supports driver segmentation and performance management.
    
    Problems Addressed:
    - #9 Hierarchical Data: Manager chain analysis
    - #32 Cohort Analysis: Driver signup cohorts
*/



with earnings as (
    select * from "uber_analytics"."dev_fact"."fct_driver_earnings"
),

drivers as (
    select * from "uber_analytics"."dev_dimension"."dim_driver"
    where is_current = true
),

-- Lifetime aggregation
lifetime_stats as (
    select
        driver_id,
        sum(total_trips) as lifetime_trips,
        sum(total_earnings_usd) as lifetime_earnings_usd,
        sum(total_tips_usd) as lifetime_tips_usd,
        avg(avg_rating_received) as avg_rating,
        min(payout_week_start) as first_trip_week,
        max(payout_week_start) as last_trip_week,
        count(distinct payout_week_start) as active_weeks
    from earnings
    group by driver_id
),

-- Recent period (last 4 weeks)
recent_stats as (
    select
        driver_id,
        sum(total_trips) as recent_trips,
        sum(total_earnings_usd) as recent_earnings_usd,
        avg(earnings_per_hour_usd) as recent_earnings_per_hour,
        avg(avg_rating_received) as recent_avg_rating
    from earnings
    where payout_week_start >= current_date - interval '28 days'
    group by driver_id
)

select
    -- Keys
    d.driver_key,
    d.driver_id,
    d.city_id,
    
    -- Driver attributes
    d.driver_status,
    d.current_rating,
    d.signup_date,
    d.background_check_status,
    d.is_active,
    
    -- Signup cohort (for cohort analysis)
    date_trunc('month', d.signup_date)::date as signup_cohort_month,
    date_trunc('quarter', d.signup_date)::date as signup_cohort_quarter,
    
    -- Tenure
    (current_date - d.signup_date) as days_since_signup,
    (current_date - d.signup_date) / 30 as months_since_signup,
    
    -- Lifetime metrics
    coalesce(l.lifetime_trips, 0) as lifetime_trips,
    coalesce(l.lifetime_earnings_usd, 0) as lifetime_earnings_usd,
    coalesce(l.lifetime_tips_usd, 0) as lifetime_tips_usd,
    coalesce(l.avg_rating, 0) as lifetime_avg_rating,
    l.first_trip_week,
    l.last_trip_week,
    coalesce(l.active_weeks, 0) as total_active_weeks,
    
    -- Recent metrics (last 4 weeks)
    coalesce(r.recent_trips, 0) as recent_trips,
    coalesce(r.recent_earnings_usd, 0) as recent_earnings_usd,
    coalesce(r.recent_earnings_per_hour, 0) as recent_earnings_per_hour,
    coalesce(r.recent_avg_rating, 0) as recent_avg_rating,
    
    -- Engagement
    case 
        when l.last_trip_week >= current_date - interval '7 days' then 'ACTIVE'
        when l.last_trip_week >= current_date - interval '30 days' then 'RECENT'
        when l.last_trip_week >= current_date - interval '90 days' then 'AT_RISK'
        else 'CHURNED'
    end as engagement_status,
    
    current_date - l.last_trip_week as days_since_last_trip,
    
    -- Performance tier
    case
        when l.lifetime_trips >= 1000 and l.avg_rating >= 4.9 then 'PLATINUM'
        when l.lifetime_trips >= 500 and l.avg_rating >= 4.8 then 'GOLD'
        when l.lifetime_trips >= 100 and l.avg_rating >= 4.7 then 'SILVER'
        else 'BRONZE'
    end as performance_tier,
    
    -- Derived metrics
    coalesce(l.lifetime_earnings_usd / nullif(l.lifetime_trips, 0), 0) as avg_earnings_per_trip,
    coalesce(l.lifetime_tips_usd / nullif(l.lifetime_earnings_usd, 0), 0) as tip_rate,
    
    -- Audit
    current_timestamp as _mart_loaded_at,
    '402658b7-059e-4de5-83c3-55909b7e2657' as _invocation_id
    
from drivers d
left join lifetime_stats l on d.driver_id = l.driver_id
left join recent_stats r on d.driver_id = r.driver_id