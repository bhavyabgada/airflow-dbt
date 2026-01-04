/*
    Model: int_trips_unified
    Layer: Integration
    Description: 
        Unified trip model combining driver and rider app data.
        Handles deduplication, late arrivals, and data enrichment.
    
    Problems Addressed:
    - #1 Late-Arriving Facts: Identifies and handles late arrivals
    - #2 Orphan Records: Validates against dimensions
    - #4 Duplicate Detection: Removes duplicates from rider app
    - #5 Timezone Handling: Converts to UTC
    - #10 Multi-Source Merge: Combines driver + rider data
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
    -- Exclude duplicates from rider app
    select * from {{ ref('stg_trips_rider_app') }}
    where _is_potential_duplicate = false
),

-- Get city timezones for conversion
cities as (
    select city_id, timezone from {{ ref('ref_cities') }}
),

-- Combine driver and rider data
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
        
        -- Timestamps (convert to UTC)
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
        
        -- Promo from rider app
        r.promo_code,
        r.promo_discount_local,
        
        -- Ratings from both sides
        d.driver_rating_received,
        r.rider_rating_given,
        r.feedback_text,
        
        -- Payment
        r.payment_method,
        
        -- Late arrival detection
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

-- Add currency conversion to USD
with_usd as (
    select
        *,
        -- Convert all monetary values to USD at transaction date
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

{% if is_incremental() %}
    where extracted_at > (select max(extracted_at) from {{ this }})
       or is_late_arrival = true  -- Always reprocess late arrivals
{% endif %}

