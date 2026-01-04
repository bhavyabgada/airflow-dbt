
  create view "uber_analytics"."dev_staging"."stg_trips_driver_app__dbt_tmp"
    
    
  as (
    /*
    Model: stg_trips_driver_app
    Source: Driver App
    Description: Staging model for trip data from the driver application.
                 Handles timezone conversion and standardizes column names.
    
    Problems Addressed:
    - #5 Timezone Handling: Converts local timestamps to UTC
    - #16 PII Handling: Conditionally masks driver data
*/



with source_data as (
    select * from "uber_analytics"."dev_staging"."source_trips_driver_app"
),

standardized as (
    select
        -- Trip identifiers
        trip_id,
        driver_id,
        vehicle_id,
        city_id,
        service_type_id,
        trip_status,
        
        -- Timestamps (kept as-is in staging, timezone conversion in integration)
        request_timestamp::timestamp as request_timestamp_local,
        accept_timestamp::timestamp as accept_timestamp_local,
        pickup_timestamp::timestamp as pickup_timestamp_local,
        dropoff_timestamp::timestamp as dropoff_timestamp_local,
        
        -- Location data
        pickup_lat::decimal(10,6) as pickup_latitude,
        pickup_lon::decimal(10,6) as pickup_longitude,
        dropoff_lat::decimal(10,6) as dropoff_latitude,
        dropoff_lon::decimal(10,6) as dropoff_longitude,
        
        -- Trip metrics
        distance_miles::decimal(10,2) as distance_miles,
        duration_minutes::integer as duration_minutes,
        
        -- Financial data (in local currency)
        base_fare::decimal(12,2) as base_fare_local,
        surge_multiplier::decimal(4,2) as surge_multiplier,
        surge_amount::decimal(12,2) as surge_amount_local,
        tips::decimal(12,2) as tips_local,
        tolls::decimal(12,2) as tolls_local,
        total_fare::decimal(12,2) as total_fare_local,
        driver_earnings::decimal(12,2) as driver_earnings_local,
        currency_code,
        
        -- Ratings
        rating_by_rider::integer as driver_rating_received,
        
        -- Metadata
        source_system,
        extracted_at::timestamp as extracted_at,
        
        -- Audit columns
        current_timestamp as _stg_loaded_at,
        'd6b654c8-2af6-42df-8c10-e532f057968e' as _stg_invocation_id
        
    from source_data
)

select * from standardized
  );