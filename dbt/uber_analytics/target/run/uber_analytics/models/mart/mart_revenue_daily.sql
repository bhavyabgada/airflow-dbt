
  
    

  create  table "uber_analytics"."dev_mart"."mart_revenue_daily__dbt_tmp"
  
  
    as
  
  (
    /*
    Model: mart_revenue_daily
    Layer: Mart
    Description: 
        Daily revenue aggregation by city with multi-currency support.
        All amounts in USD with fiscal calendar attributes.
    
    Problems Addressed:
    - #7 Currency Conversion: Aggregated in USD
    - #8 Fiscal Calendar: Includes fiscal year/quarter by region
    - #12 Conditional Aggregations: Excludes cancellations, includes refund adjustments
*/



with trips as (
    select * from "uber_analytics"."dev_fact"."fct_trips"
    where trip_status = 'TRIP_COMPLETED'
),

-- Get city info for fiscal calendar
cities as (
    select * from "uber_analytics"."dev_staging"."ref_cities"
),

-- Get date dimension for fiscal attributes
dates as (
    select * from "uber_analytics"."dev_dimension"."dim_date"
),

-- Daily aggregation by city
daily_revenue as (
    select
        t.request_date,
        t.city_id,
        c.country_code,
        c.region,
        
        -- Trip counts
        count(*) as completed_trips,
        count(case when t.surge_multiplier > 1.0 then 1 end) as surge_trips,
        count(case when t.tips_usd > 0 then 1 end) as tipped_trips,
        
        -- Gross revenue (USD)
        sum(t.total_fare_usd) as gross_bookings_usd,
        sum(t.base_fare_usd) as base_fare_usd,
        sum(t.surge_amount_usd) as surge_revenue_usd,
        sum(t.tips_usd) as tips_usd,
        sum(t.tolls_usd) as tolls_usd,
        sum(t.promo_discount_usd) as promo_discounts_usd,
        
        -- Driver payouts
        sum(t.driver_earnings_usd) as driver_payouts_usd,
        
        -- Uber net revenue (Gross - Driver payouts)
        sum(t.total_fare_usd) - sum(t.driver_earnings_usd) as uber_net_revenue_usd,
        
        -- Averages
        avg(t.total_fare_usd) as avg_fare_usd,
        avg(t.distance_miles) as avg_distance_miles,
        avg(t.duration_minutes) as avg_duration_minutes,
        avg(t.driver_rating_received) as avg_driver_rating,
        
        -- Late arrivals (data quality indicator)
        count(case when t.is_late_arrival then 1 end) as late_arrival_trips,
        
        -- Last update
        max(t._fact_loaded_at) as last_updated_at
        
    from trips t
    left join cities c on t.city_id = c.city_id
    group by 1, 2, 3, 4
)

select
    -- Keys
    
    md5(
        
            coalesce(cast(request_date as varchar), '_NULL_')
             || '|' || 
        
            coalesce(cast(city_id as varchar), '_NULL_')
            
        
    )
 as revenue_key,
    dr.request_date,
    dr.city_id,
    dr.country_code,
    dr.region,
    
    -- Date key for joins
    to_char(dr.request_date, 'YYYYMMDD')::integer as date_key,
    
    -- Calendar attributes
    d.calendar_year,
    d.calendar_quarter,
    d.calendar_month,
    d.calendar_week,
    d.day_of_week,
    d.is_weekend,
    
    -- Fiscal attributes (varies by region)
    case 
        when dr.region in ('EMEA', 'APAC') then d.fiscal_year_uk
        when dr.region = 'Australia' then d.fiscal_year_au
        else d.fiscal_year_us
    end as fiscal_year,
    
    case 
        when dr.region in ('EMEA', 'APAC') then d.fiscal_quarter_uk
        when dr.region = 'Australia' then d.fiscal_quarter_au
        else d.fiscal_quarter_us
    end as fiscal_quarter,
    
    -- Trip metrics
    dr.completed_trips,
    dr.surge_trips,
    dr.tipped_trips,
    dr.late_arrival_trips,
    
    -- Revenue metrics (USD)
    dr.gross_bookings_usd,
    dr.base_fare_usd,
    dr.surge_revenue_usd,
    dr.tips_usd,
    dr.tolls_usd,
    dr.promo_discounts_usd,
    dr.driver_payouts_usd,
    dr.uber_net_revenue_usd,
    
    -- Averages
    dr.avg_fare_usd,
    dr.avg_distance_miles,
    dr.avg_duration_minutes,
    dr.avg_driver_rating,
    
    -- Derived ratios
    dr.surge_trips::decimal / nullif(dr.completed_trips, 0) as surge_trip_rate,
    dr.tipped_trips::decimal / nullif(dr.completed_trips, 0) as tipped_trip_rate,
    dr.uber_net_revenue_usd / nullif(dr.gross_bookings_usd, 0) as take_rate,
    
    -- Audit
    dr.last_updated_at,
    current_timestamp as _mart_loaded_at,
    '2a9b2b56-d1bf-499d-92f6-dd6e735dddce' as _invocation_id
    
from daily_revenue dr
left join dates d on dr.request_date = d.full_date
  );
  