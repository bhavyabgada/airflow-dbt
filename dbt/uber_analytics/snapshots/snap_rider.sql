/*
    Snapshot: snap_rider
    Type: SCD Type 2
    Description: 
        Tracks historical changes to rider profiles.
        Enables point-in-time lookups for customer analytics.
    
    Problems Addressed:
    - #6 Point-in-Time Joins: Rider tier at time of trip
*/

{% snapshot snap_rider %}

{{
    config(
        target_schema='dimension',
        unique_key='rider_id',
        strategy='check',
        check_cols=['rider_tier', 'city_id', 'preferred_payment_method', 'is_active', 'fraud_flag'],
        invalidate_hard_deletes=True
    )
}}

select
    rider_id,
    
    {% if var('mask_pii', true) %}
    first_name_hash,
    last_name_hash,
    email_hash,
    phone_hash,
    {% else %}
    first_name,
    last_name,
    email,
    phone,
    {% endif %}
    
    date_of_birth,
    city_id,
    rider_tier,
    lifetime_trips,
    lifetime_spend_usd,
    signup_date,
    last_trip_date,
    preferred_payment_method,
    is_active,
    fraud_flag,
    data_retention_until,
    extracted_at as updated_at
    
from {{ ref('stg_riders') }}

{% endsnapshot %}

