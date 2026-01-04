/*
================================================================================
Model: stg_trips_rider_app
Layer: Staging
Source: Rider App
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#4 DUPLICATE DETECTION
    Problem: Mobile apps can submit duplicate requests due to:
             - Network retries (request sent twice due to timeout)
             - User double-taps (impatient user clicks multiple times)
             - App bugs (state management issues)
             - Offline sync (cached requests uploaded twice)
    
             Same trip gets recorded 2+ times with slightly different timestamps.
             Without deduplication:
             - Revenue counted twice
             - Trip metrics inflated
             - Customer charged multiple times (worst case!)
    
    Example Duplicates:
    | rider_id | pickup_address | request_time | trip_id |
    |----------|----------------|--------------|---------|
    | R001     | 123 Main St    | 10:00:00     | T-001   | <- Original
    | R001     | 123 Main St    | 10:00:02     | T-002   | <- Duplicate (2s later)
    | R001     | 123 Main St    | 10:00:15     | T-003   | <- Another duplicate
    
    Business Impact: Overstated revenue, poor customer experience, inflated KPIs.

--------------------------------------------------------------------------------

SOLUTION: ROW_NUMBER() DEDUPLICATION
    
    Strategy: Keep FIRST occurrence (by extracted_at), flag others as duplicates.
    
    1. Define what makes a "duplicate":
       - Same rider_id
       - Same pickup location (lat/long rounded to ~100m)
       - Within same 5-minute window
    
    2. Partition by these criteria, order by extracted_at:
       ```sql
       ROW_NUMBER() OVER (
           PARTITION BY 
               rider_id,
               date_trunc('minute', request_timestamp),
               round(pickup_latitude, 3),
               round(pickup_longitude, 3)
           ORDER BY extracted_at
       ) as _dedup_rank
       ```
    
    3. First row (_dedup_rank = 1) is the original, others are duplicates
    
    4. Flag duplicates for tracking (don't delete - need audit trail):
       ```sql
       _dedup_rank > 1 as _is_potential_duplicate
       ```

--------------------------------------------------------------------------------

WHY TRACK DUPLICATES (NOT DELETE)?
    
    - Audit trail: Know how many duplicates exist
    - Root cause analysis: Identify app bugs by duplicate patterns
    - Reconciliation: Verify we're not undercounting
    - Compliance: Can't delete without retention policy
    
    Downstream models filter: WHERE _is_potential_duplicate = false

================================================================================
*/

{{ config(
    materialized='view',
    tags=['staging', 'trips', 'rider']
) }}

with source_data as (
    select * from {{ ref('source_trips_rider_app') }}
),

-- PROBLEM #4: Add deduplication rank
with_dedup as (
    select
        *,
        -- Identify duplicates: same rider, same location (rounded), same minute
        row_number() over (
            partition by 
                rider_id,
                -- Same minute bucket
                date_trunc('minute', request_timestamp::timestamp),
                -- Same pickup location (rounded to ~100m precision)
                round(pickup_latitude::decimal, 3),
                round(pickup_longitude::decimal, 3)
            order by extracted_at::timestamp  -- First extracted wins
        ) as _dedup_rank
    from source_data
),

standardized as (
    select
        -- Trip identifiers
        trip_id,
        rider_id,
        
        -- Timestamps (kept in local time, conversion happens in integration)
        request_timestamp::timestamp as request_timestamp_local,
        
        -- Location
        pickup_latitude::decimal(10,6) as pickup_latitude,
        pickup_longitude::decimal(10,6) as pickup_longitude,
        pickup_address,
        dropoff_latitude::decimal(10,6) as dropoff_latitude,
        dropoff_longitude::decimal(10,6) as dropoff_longitude,
        dropoff_address,
        
        -- Service & status
        service_type_id,
        trip_status,
        
        -- Promo tracking
        promo_code,
        promo_discount_local::decimal(10,2) as promo_discount_local,
        
        -- Payment
        payment_method,
        
        -- Ratings (rider gave to driver)
        driver_rating_given::decimal(2,1) as rider_rating_given,
        feedback_text,
        
        -- PROBLEM #4: Duplicate detection flag
        -- First occurrence (_dedup_rank = 1) is NOT a duplicate
        -- All subsequent occurrences ARE potential duplicates
        _dedup_rank > 1 as _is_potential_duplicate,
        _dedup_rank,
        
        -- Metadata
        source_system,
        extracted_at::timestamp as extracted_at,
        
        -- Audit
        current_timestamp as _stg_loaded_at,
        '{{ invocation_id }}' as _stg_invocation_id
        
    from with_dedup
)

select * from standardized
