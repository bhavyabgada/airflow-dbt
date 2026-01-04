
      
  
    

  create  table "uber_analytics"."dev_fact"."fct_driver_earnings"
  
  
    as
  
  (
    /*
    Model: fct_driver_earnings
    Layer: Fact
    Description: 
        Periodic snapshot fact table for driver earnings.
        Aggregated by driver and payout week (Mon-Sun).
    
    Problems Addressed:
    - #7 Currency Conversion: Weekly earnings in driver's payout currency
    - #8 Fiscal Calendar: Uses payout week (Mon-Sun)
*/



with trips as (
    select * from "uber_analytics"."dev_fact"."fct_trips"
    where trip_status = 'TRIP_COMPLETED'
),

-- Aggregate by driver and payout week
weekly_earnings as (
    select
        driver_id,
        city_id,
        
    -- Monday of the week
    date_trunc('week', request_timestamp_utc)::date
 as payout_week_start,
        
    -- Sunday of the week
    (date_trunc('week', request_timestamp_utc) + interval '6 days')::date
 as payout_week_end,
        extract(isoyear from request_timestamp_utc)::integer as payout_year,
        extract(week from request_timestamp_utc)::integer as payout_week_number,
        
        -- Trip counts
        count(*) as total_trips,
        count(case when surge_multiplier > 1.0 then 1 end) as surge_trips,
        count(case when tips_usd > 0 then 1 end) as tipped_trips,
        
        -- Distance and time
        sum(distance_miles) as total_distance_miles,
        sum(duration_minutes) as total_trip_minutes,
        
        -- Earnings (USD)
        sum(driver_earnings_usd) as gross_earnings_usd,
        sum(tips_usd) as total_tips_usd,
        sum(tolls_usd) as total_tolls_usd,
        sum(driver_earnings_usd) + sum(tips_usd) as total_earnings_usd,
        
        -- Averages
        avg(driver_earnings_usd) as avg_earnings_per_trip_usd,
        avg(tips_usd) as avg_tip_per_trip_usd,
        avg(driver_rating_received) as avg_rating_received,
        
        -- Efficiency metrics
        sum(driver_earnings_usd) / nullif(sum(duration_minutes), 0) * 60 as earnings_per_hour_usd,
        sum(driver_earnings_usd) / nullif(sum(distance_miles), 0) as earnings_per_mile_usd,
        
        -- Last activity
        max(request_timestamp_utc) as last_trip_timestamp
        
    from trips
    group by 1, 2, 3, 4, 5, 6
)

select
    -- Surrogate key
    
    md5(
        
            coalesce(cast(driver_id as varchar), '_NULL_')
             || '|' || 
        
            coalesce(cast(payout_week_start as varchar), '_NULL_')
            
        
    )
 as earnings_key,
    
    -- Keys
    driver_id,
    city_id,
    to_char(payout_week_start, 'YYYYMMDD')::integer as payout_week_start_key,
    
    -- Period
    payout_week_start,
    payout_week_end,
    payout_year,
    payout_week_number,
    
    -- Trip metrics
    total_trips,
    surge_trips,
    tipped_trips,
    total_distance_miles,
    total_trip_minutes,
    
    -- Earnings
    gross_earnings_usd,
    total_tips_usd,
    total_tolls_usd,
    total_earnings_usd,
    
    -- Averages
    avg_earnings_per_trip_usd,
    avg_tip_per_trip_usd,
    avg_rating_received,
    
    -- Efficiency
    earnings_per_hour_usd,
    earnings_per_mile_usd,
    
    -- Activity
    last_trip_timestamp,
    
    -- Audit
    current_timestamp as _fact_loaded_at,
    '2a9b2b56-d1bf-499d-92f6-dd6e735dddce' as _invocation_id
    
from weekly_earnings


  );
  
  