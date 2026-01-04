/*
    Model: stg_trips_rider_app
    Source: Rider App
    Description: Staging model for trip data from the rider application.
                 Contains rider-specific fields like ratings given and feedback.
    
    Problems Addressed:
    - #4 Duplicate Detection: Marks potential duplicates (same rider, same time)
    - #16 PII Handling: Masks rider contact info if var set
*/

{{ config(
    materialized='view',
    tags=['staging', 'trips', 'rider_app']
) }}

with source_data as (
    select * from {{ ref('source_trips_rider_app') }}
),

-- Detect potential duplicates (rider app can send same request twice)
with_duplicate_flag as (
    select
        *,
        row_number() over (
            partition by 
                -- Same rider, same pickup location, within 5 minutes
                rider_id, 
                pickup_address,
                date_trunc('minute', request_timestamp::timestamp)
            order by extracted_at::timestamp
        ) as _duplicate_rank
    from source_data
),

standardized as (
    select
        -- Trip identifiers
        trip_id,
        rider_id,
        city_id,
        service_type_id,
        trip_status,
        
        -- Timestamps
        request_timestamp::timestamp as request_timestamp_local,
        
        -- Addresses (may contain PII)
        {% if var('mask_pii', true) %}
        md5(pickup_address) as pickup_address_hash,
        md5(dropoff_address) as dropoff_address_hash,
        {% else %}
        pickup_address,
        dropoff_address,
        {% endif %}
        
        -- Financial data
        estimated_fare::decimal(12,2) as estimated_fare_local,
        actual_fare::decimal(12,2) as actual_fare_local,
        payment_method,
        promo_code,
        promo_discount::decimal(12,2) as promo_discount_local,
        
        -- Ratings & Feedback
        rating_given_to_driver::integer as rider_rating_given,
        feedback_text,
        
        -- Duplicate detection
        _duplicate_rank,
        case when _duplicate_rank > 1 then true else false end as _is_potential_duplicate,
        
        -- Metadata
        source_system,
        extracted_at::timestamp as extracted_at,
        
        -- Audit columns
        current_timestamp as _stg_loaded_at,
        '{{ invocation_id }}' as _stg_invocation_id
        
    from with_duplicate_flag
)

select * from standardized

