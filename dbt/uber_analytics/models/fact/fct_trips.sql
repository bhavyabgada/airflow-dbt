/*
    Model: fct_trips
    Layer: Fact
    Description: 
        Transaction grain fact table for trips.
        One row per trip with all dimensional keys and measures.
    
    Problems Addressed:
    - #1 Late-Arriving Facts: Reprocesses late arrivals
    - #6 Point-in-Time: Links to driver version at trip time
    - #7 Currency Conversion: All amounts in USD
    - #13 Backfill: Supports historical reprocessing
    - #17 Large Table Joins: Optimized with surrogate keys
    - #19 Partitioning: Partitioned by request date
    - #20 Role-Playing: Multiple date dimension joins
    - #25 Junk Dimension: Uses trip_flag_key
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

-- Get driver at time of trip (point-in-time SCD2 lookup)
drivers as (
    select * from {{ ref('dim_driver') }}
),

-- Get rider at time of trip
riders as (
    select * from {{ ref('dim_rider') }}
),

-- Get trip flag dimension key
trip_flags as (
    select * from {{ ref('dim_trip_flags') }}
),

-- Build fact table
fact as (
    select
        -- Primary key
        t.trip_id,
        
        -- Dimension keys (for star schema joins)
        -- Date keys (role-playing)
        to_char(t.request_timestamp_utc::date, 'YYYYMMDD')::integer as request_date_key,
        to_char(t.pickup_timestamp_utc::date, 'YYYYMMDD')::integer as pickup_date_key,
        to_char(t.dropoff_timestamp_utc::date, 'YYYYMMDD')::integer as dropoff_date_key,
        
        -- Driver key (point-in-time)
        d.driver_key,
        t.driver_id,
        
        -- Rider key (point-in-time)
        r.rider_key,
        t.rider_id,
        
        -- Vehicle
        t.vehicle_id,
        
        -- Geography
        t.city_id,
        
        -- Service type
        t.service_type_id,
        
        -- Trip flag (junk dimension)
        tf.trip_flag_key,
        
        -- Degenerate dimensions
        t.trip_status,
        t.promo_code,
        t.payment_method,
        
        -- Timestamps (UTC)
        t.request_timestamp_utc,
        t.accept_timestamp_utc,
        t.pickup_timestamp_utc,
        t.dropoff_timestamp_utc,
        
        -- Date (for partitioning)
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
        
        -- Financial measures (local currency)
        t.currency_code,
        t.base_fare_local,
        t.surge_amount_local,
        t.tips_local,
        t.tolls_local,
        t.promo_discount_local,
        t.total_fare_local,
        t.driver_earnings_local,
        
        -- Financial measures (USD - for aggregation)
        t.base_fare_usd,
        t.surge_amount_usd,
        t.tips_usd,
        t.tolls_usd,
        t.promo_discount_usd,
        t.total_fare_usd,
        t.driver_earnings_usd,
        
        -- Calculated measures
        t.total_fare_usd * st.commission_rate as uber_commission_usd,
        t.total_fare_usd - t.driver_earnings_usd as uber_net_revenue_usd,
        
        -- Ratings
        t.driver_rating_received,
        t.rider_rating_given,
        
        -- Driver rating at time of trip (point-in-time)
        d.current_rating as driver_rating_at_trip,
        
        -- Rider tier at time of trip
        r.rider_tier as rider_tier_at_trip,
        
        -- Operational flags
        t.is_late_arrival,
        
        -- Time metrics
        extract(epoch from (t.accept_timestamp_utc - t.request_timestamp_utc))/60 as time_to_accept_minutes,
        extract(epoch from (t.pickup_timestamp_utc - t.accept_timestamp_utc))/60 as time_to_pickup_minutes,
        
        -- Audit
        t.extracted_at,
        t._int_loaded_at,
        current_timestamp as _fact_loaded_at,
        '{{ invocation_id }}' as _invocation_id
        
    from trips t
    left join service_types st on t.service_type_id = st.service_type_id
    -- Point-in-time join to driver (driver version valid at trip time)
    left join drivers d on t.driver_id = d.driver_id
        and t.request_timestamp_utc >= d.valid_from
        and (t.request_timestamp_utc < d.valid_to or d.valid_to is null)
    -- Point-in-time join to rider
    left join riders r on t.rider_id = r.rider_id
        and t.request_timestamp_utc >= r.valid_from
        and (t.request_timestamp_utc < r.valid_to or r.valid_to is null)
    -- Join to junk dimension
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

{% if is_incremental() %}
    where extracted_at > (select max(extracted_at) from {{ this }})
       or is_late_arrival = true  -- Always reprocess late arrivals
{% endif %}

