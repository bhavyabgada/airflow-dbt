/*
================================================================================
Model: fct_trips
Layer: Fact
Grain: One row per trip
================================================================================

PROBLEMS SOLVED:
--------------------------------------------------------------------------------

#6 POINT-IN-TIME JOINS (SCD Type 2)
    Problem: Driver attributes change over time (rating, city, status). When 
             analyzing a trip from 3 months ago, we need the driver's rating
             AT THAT TIME, not their current rating.
    
    Example: 
    - Driver D101 had 4.5 rating in January, now has 4.9 rating
    - Trip T001 happened on Jan 15
    - Report should show 4.5, not 4.9
    
    Business Impact: Wrong performance analysis, incorrect incentive calculations.
    
    Solution:
    - dim_driver has valid_from and valid_to columns (SCD Type 2)
    - Join using: WHERE trip_timestamp >= valid_from 
                    AND (trip_timestamp < valid_to OR valid_to IS NULL)
    - This ensures we get the driver version that was active at trip time
    
    Code Pattern:
    ```sql
    left join dim_driver d on t.driver_id = d.driver_id
        and t.request_timestamp_utc >= d.valid_from
        and (t.request_timestamp_utc < d.valid_to or d.valid_to is null)
    ```

--------------------------------------------------------------------------------

#16 LARGE TABLE OPTIMIZATION
    Problem: Trip fact table can have billions of rows. Full table scans for
             queries like "total revenue last week" are expensive.
    
    Business Impact: Slow dashboards, high compute costs.
    
    Solution:
    - Use SURROGATE KEYS (integers/hashes) instead of natural keys
    - Partition by request_date (most common filter)
    - Date keys as integers (20240115) for efficient filtering
    - Pre-calculate common derived metrics

--------------------------------------------------------------------------------

#17 ROLE-PLAYING DIMENSIONS
    Problem: A trip has THREE relevant dates - request, pickup, and dropoff.
             Each needs to join to the date dimension for different analyses:
             - "Revenue by request date" (when customer ordered)
             - "Revenue by pickup date" (when trip started)
             - "Revenue by dropoff date" (when trip ended)
    
    Business Impact: Need separate joins for different date analyses.
    
    Solution:
    - Include THREE date keys: request_date_key, pickup_date_key, dropoff_date_key
    - All reference the SAME dim_date table
    - BI tools can join to any based on analysis need
    
    Code Pattern:
    ```sql
    to_char(request_timestamp_utc::date, 'YYYYMMDD')::integer as request_date_key,
    to_char(pickup_timestamp_utc::date, 'YYYYMMDD')::integer as pickup_date_key,
    to_char(dropoff_timestamp_utc::date, 'YYYYMMDD')::integer as dropoff_date_key,
    ```

--------------------------------------------------------------------------------

#19 JUNK DIMENSION USAGE
    Problem: Trips have many boolean flags (is_surge, is_pool, has_tip, etc.).
             Storing 8 boolean columns wastes space and complicates queries.
    
    Business Impact: Wide tables, complex WHERE clauses.
    
    Solution:
    - Create dim_trip_flags with all 192 possible combinations (2^8 minus invalid)
    - Store single trip_flag_key in fact table
    - Join to junk dimension for filtering/grouping
    
    Query becomes: WHERE tf.is_surge = true AND tf.has_tip = true
    Instead of: WHERE is_surge = true AND has_tip = true (on fact)

--------------------------------------------------------------------------------

#13 BACKFILL SUPPORT
    Problem: Historical data needs to be reprocessed when:
             - Bug fix in transformation logic
             - New column added
             - Exchange rates corrected
    
    Business Impact: Unable to fix historical data without manual intervention.
    
    Solution:
    - Model supports both incremental AND full-refresh modes
    - `dbt run --full-refresh` rebuilds entire table
    - Date partition allows targeted backfills: `--vars '{"execution_date": "2024-01-15"}'`

================================================================================
*/

{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    partition_by={
        'field': 'request_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    tags=['fact', 'trips']
) }}

with trips as (
    select * from {{ ref('int_trips_unified') }}
),

-- Get service type attributes
service_types as (
    select * from {{ ref('ref_service_types') }}
),

-- PROBLEM #6: Get driver at time of trip (point-in-time SCD2 lookup)
drivers as (
    select * from {{ ref('dim_driver') }}
),

-- Get rider at time of trip
riders as (
    select * from {{ ref('dim_rider') }}
),

-- PROBLEM #19: Get trip flag dimension key
trip_flags as (
    select * from {{ ref('dim_trip_flags') }}
),

-- Build fact table
fact as (
    select
        -- Primary key
        t.trip_id,
        
        -- PROBLEM #17: Role-playing date dimension keys
        -- Same dim_date table, three different uses
        to_char(t.request_timestamp_utc::date, 'YYYYMMDD')::integer as request_date_key,
        to_char(t.pickup_timestamp_utc::date, 'YYYYMMDD')::integer as pickup_date_key,
        to_char(t.dropoff_timestamp_utc::date, 'YYYYMMDD')::integer as dropoff_date_key,
        
        -- PROBLEM #6: Driver key (point-in-time version)
        d.driver_key,
        t.driver_id,
        
        -- Rider key (point-in-time version)
        r.rider_key,
        t.rider_id,
        
        -- Vehicle
        t.vehicle_id,
        
        -- Geography
        t.city_id,
        
        -- Service type
        t.service_type_id,
        
        -- PROBLEM #19: Trip flag (junk dimension key)
        tf.trip_flag_key,
        
        -- Degenerate dimensions (low cardinality, no need for separate dim)
        t.trip_status,
        t.promo_code,
        t.payment_method,
        
        -- Timestamps (UTC for consistent analytics)
        t.request_timestamp_utc,
        t.accept_timestamp_utc,
        t.pickup_timestamp_utc,
        t.dropoff_timestamp_utc,
        
        -- PROBLEM #16: Partition key for large table optimization
        t.request_timestamp_utc::date as request_date,
        
        -- Location measures
        t.pickup_latitude,
        t.pickup_longitude,
        t.dropoff_latitude,
        t.dropoff_longitude,
        
        -- Trip metrics
        t.distance_miles,
        t.duration_minutes,
        t.surge_multiplier,
        
        -- Financial measures (local currency - for driver payouts)
        t.currency_code,
        t.base_fare_local,
        t.surge_amount_local,
        t.tips_local,
        t.tolls_local,
        t.promo_discount_local,
        t.total_fare_local,
        t.driver_earnings_local,
        
        -- Financial measures (USD - for global aggregation)
        t.base_fare_usd,
        t.surge_amount_usd,
        t.tips_usd,
        t.tolls_usd,
        t.promo_discount_usd,
        t.total_fare_usd,
        t.driver_earnings_usd,
        
        -- Pre-calculated measures (avoid recalculation in BI tools)
        t.total_fare_usd * st.commission_rate as uber_commission_usd,
        t.total_fare_usd - t.driver_earnings_usd as uber_net_revenue_usd,
        
        -- Ratings
        t.driver_rating_received,
        t.rider_rating_given,
        
        -- PROBLEM #6: Driver rating AT TIME OF TRIP (not current!)
        d.current_rating as driver_rating_at_trip,
        
        -- Rider tier at time of trip
        r.rider_tier as rider_tier_at_trip,
        
        -- Operational flags
        t.is_late_arrival,
        
        -- Time metrics (pre-calculated for efficiency)
        extract(epoch from (t.accept_timestamp_utc - t.request_timestamp_utc))/60 as time_to_accept_minutes,
        extract(epoch from (t.pickup_timestamp_utc - t.accept_timestamp_utc))/60 as time_to_pickup_minutes,
        
        -- Audit
        t.extracted_at,
        t._int_loaded_at,
        current_timestamp as _fact_loaded_at,
        '{{ invocation_id }}' as _invocation_id
        
    from trips t
    left join service_types st on t.service_type_id = st.service_type_id
    
    -- PROBLEM #6: Point-in-time join to driver dimension
    -- Gets the driver version that was valid when the trip occurred
    left join drivers d on t.driver_id = d.driver_id
        and t.request_timestamp_utc >= d.valid_from
        and (t.request_timestamp_utc < d.valid_to or d.valid_to is null)
    
    -- Point-in-time join to rider dimension
    left join riders r on t.rider_id = r.rider_id
        and t.request_timestamp_utc >= r.valid_from
        and (t.request_timestamp_utc < r.valid_to or r.valid_to is null)
    
    -- PROBLEM #19: Join to junk dimension based on flag values
    left join trip_flags tf on 
        tf.is_surge = (t.surge_multiplier > 1.0)
        and tf.is_pool = (t.service_type_id = 'UBER_POOL')
        and tf.is_premium = (t.service_type_id in ('UBER_BLACK', 'UBER_SUV'))
        and tf.has_tip = (t.tips_usd > 0)
        and tf.has_promo = (t.promo_code is not null)
        and tf.is_late_arrival = t.is_late_arrival
        and tf.is_completed = (t.trip_status = 'TRIP_COMPLETED')
        and tf.is_cancelled = (t.trip_status like 'CANCELLED%')
)

select * from fact

-- PROBLEM #13: Incremental logic (also supports full refresh)
{% if is_incremental() %}
    where extracted_at > (select max(extracted_at) from {{ this }})
       or is_late_arrival = true  -- Always reprocess late arrivals
{% endif %}
