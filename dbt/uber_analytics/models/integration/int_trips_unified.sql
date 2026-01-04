/*
================================================================================
Model: int_trips_unified
Layer: Integration
================================================================================

PROBLEMS SOLVED:
--------------------------------------------------------------------------------

#1 LATE-ARRIVING FACTS
    Problem: Trip data from offline driver devices can arrive 24-48 hours late.
             For example, a driver in a subway or rural area syncs their app
             later, causing trip records to arrive after the daily pipeline ran.
    
    Business Impact: Missing revenue in daily reports, incorrect driver payouts.
    
    Solution: 
    - Add `is_late_arrival` flag by comparing event time vs extraction time
    - In incremental logic, ALWAYS reprocess late arrivals regardless of
      extraction timestamp
    - Use: {{ is_late_arrival('request_timestamp', 'extracted_at', 24) }}
    
    Code Pattern:
    ```sql
    {% if is_incremental() %}
        where extracted_at > (select max(extracted_at) from {{ this }})
           or is_late_arrival = true  -- Always reprocess late arrivals
    {% endif %}
    ```

--------------------------------------------------------------------------------

#4 DUPLICATE DETECTION  
    Problem: Rider app can submit the same trip request multiple times due to
             network retries, double-taps, or app bugs. Without deduplication,
             we'd count revenue twice.
    
    Business Impact: Inflated trip counts, double-charged customers.
    
    Solution:
    - In stg_trips_rider_app, use ROW_NUMBER() partitioned by (rider_id, 
      pickup_address, minute of request)
    - Only join records where _is_potential_duplicate = false
    - First record (by extracted_at) wins

--------------------------------------------------------------------------------

#5 TIMEZONE HANDLING
    Problem: Uber operates in 15+ cities across different timezones (NYC, Tokyo,
             London, etc.). Trip timestamps are stored in local time, but analytics
             need UTC for global aggregations.
    
    Business Impact: Incorrect "trips per hour" metrics, wrong peak hours.
    
    Solution:
    - Store city timezone in ref_cities seed
    - Convert all timestamps to UTC using: 
      `timestamp AT TIME ZONE city_timezone AT TIME ZONE 'UTC'`
    - Keep both local and UTC versions for different use cases

--------------------------------------------------------------------------------

#7 CURRENCY CONVERSION
    Problem: Trips are charged in local currency (USD, GBP, JPY, MXN, etc.) but
             financial reporting needs USD. Exchange rates fluctuate daily.
    
    Business Impact: Wrong revenue numbers if using today's rate for old trips.
    
    Solution:
    - Use POINT-IN-TIME exchange rates from ref_currency_rates
    - Convert at the transaction date, not current date
    - Macro: {{ convert_to_usd('amount', 'currency', 'transaction_date') }}
    - Fallback: If exact date missing, use most recent rate before that date

--------------------------------------------------------------------------------

#10 MULTI-SOURCE MERGE (Golden Record)
    Problem: Trip data comes from TWO sources - Driver App and Rider App.
             Each has different fields. Need to create one "golden" trip record.
    
    - Driver App: Has earnings, vehicle, route details, rating RECEIVED
    - Rider App: Has promo codes, feedback, payment method, rating GIVEN
    
    Business Impact: Incomplete trip records, missing promo attribution.
    
    Solution:
    - Use Driver App as PRIMARY source (has financial data)
    - LEFT JOIN Rider App to enrich with rider-specific fields
    - Handle NULLs gracefully with COALESCE where appropriate

--------------------------------------------------------------------------------

#14 IDEMPOTENCY
    Problem: Pipeline might run multiple times (retries, backfills). Running
             twice shouldn't create duplicate records or corrupt data.
    
    Business Impact: Duplicate records, incorrect aggregations.
    
    Solution:
    - Use `unique_key='trip_id'` in incremental config
    - Use `incremental_strategy='merge'` to UPSERT not INSERT
    - Same input → Same output, always

================================================================================
*/

{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    tags=['integration', 'trips']
) }}

with driver_trips as (
    select * from {{ ref('stg_trips_driver_app') }}
),

rider_trips as (
    -- PROBLEM #4: Exclude duplicates detected in staging
    select * from {{ ref('stg_trips_rider_app') }}
    where _is_potential_duplicate = false
),

-- Get city timezones for conversion
cities as (
    select city_id, timezone from {{ ref('ref_cities') }}
),

-- PROBLEM #10: Combine driver and rider data (Golden Record)
combined as (
    select
        -- Use driver app as primary source
        d.trip_id,
        d.driver_id,
        r.rider_id,
        d.vehicle_id,
        d.city_id,
        d.service_type_id,
        d.trip_status,
        
        -- PROBLEM #5: Timestamps (convert to UTC)
        d.request_timestamp_local,
        d.request_timestamp_local at time zone c.timezone at time zone 'UTC' as request_timestamp_utc,
        d.accept_timestamp_local at time zone c.timezone at time zone 'UTC' as accept_timestamp_utc,
        d.pickup_timestamp_local at time zone c.timezone at time zone 'UTC' as pickup_timestamp_utc,
        d.dropoff_timestamp_local at time zone c.timezone at time zone 'UTC' as dropoff_timestamp_utc,
        
        -- Location
        d.pickup_latitude,
        d.pickup_longitude,
        d.dropoff_latitude,
        d.dropoff_longitude,
        
        -- Metrics
        d.distance_miles,
        d.duration_minutes,
        
        -- Financial (local currency)
        d.base_fare_local,
        d.surge_multiplier,
        d.surge_amount_local,
        d.tips_local,
        d.tolls_local,
        d.total_fare_local,
        d.driver_earnings_local,
        d.currency_code,
        
        -- Promo from rider app (enrichment)
        r.promo_code,
        r.promo_discount_local,
        
        -- Ratings from both sides
        d.driver_rating_received,
        r.rider_rating_given,
        r.feedback_text,
        
        -- Payment
        r.payment_method,
        
        -- PROBLEM #1: Late arrival detection
        -- If data arrived >24 hours after the event, flag it
        {{ is_late_arrival('d.request_timestamp_local', 'd.extracted_at', 24) }} as is_late_arrival,
        
        -- Metadata
        d.source_system as driver_source,
        r.source_system as rider_source,
        greatest(d.extracted_at, coalesce(r.extracted_at, d.extracted_at)) as extracted_at,
        
        -- Audit
        current_timestamp as _int_loaded_at,
        '{{ invocation_id }}' as _int_invocation_id
        
    from driver_trips d
    left join rider_trips r on d.trip_id = r.trip_id
    left join cities c on d.city_id = c.city_id
),

-- PROBLEM #7: Add currency conversion to USD at transaction date
with_usd as (
    select
        *,
        -- Convert all monetary values to USD using transaction-date exchange rate
        {{ convert_to_usd('base_fare_local', 'currency_code', 'request_timestamp_local') }} as base_fare_usd,
        {{ convert_to_usd('surge_amount_local', 'currency_code', 'request_timestamp_local') }} as surge_amount_usd,
        {{ convert_to_usd('tips_local', 'currency_code', 'request_timestamp_local') }} as tips_usd,
        {{ convert_to_usd('tolls_local', 'currency_code', 'request_timestamp_local') }} as tolls_usd,
        {{ convert_to_usd('total_fare_local', 'currency_code', 'request_timestamp_local') }} as total_fare_usd,
        {{ convert_to_usd('driver_earnings_local', 'currency_code', 'request_timestamp_local') }} as driver_earnings_usd,
        {{ convert_to_usd('promo_discount_local', 'currency_code', 'request_timestamp_local') }} as promo_discount_usd
    from combined
)

select * from with_usd

-- PROBLEM #1 & #14: Incremental logic with late arrival handling
{% if is_incremental() %}
    where extracted_at > (select max(extracted_at) from {{ this }})
       or is_late_arrival = true  -- Always reprocess late arrivals for accuracy
{% endif %}
