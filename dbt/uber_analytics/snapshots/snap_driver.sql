/*
    Snapshot: snap_driver
    Type: SCD Type 2
    Description: 
        Tracks historical changes to driver attributes.
        Creates new version when any tracked attribute changes.
    
    Problems Addressed:
    - #6 Point-in-Time Joins: Driver rating at time of trip
    - #11 Slowly Changing Dimensions: Track driver changes over time
*/

{% snapshot snap_driver %}

{{
    config(
        target_schema='dimension',
        unique_key='driver_id',
        strategy='check',
        check_cols=['driver_status', 'current_rating', 'city_id', 'license_expiry', 'is_active'],
        invalidate_hard_deletes=True
    )
}}

select
    driver_id,
    
    -- PII (hashed in staging)
    {% if var('mask_pii', true) %}
    first_name_hash,
    last_name_hash,
    email_hash,
    phone_hash,
    ssn_masked,
    {% else %}
    first_name,
    last_name,
    email,
    phone,
    ssn_last_four,
    {% endif %}
    
    date_of_birth,
    license_number,
    license_state,
    license_expiry,
    city_id,
    driver_status,
    current_rating,
    total_trips,
    signup_date,
    last_active_date,
    background_check_status,
    background_check_date,
    is_active,
    _row_hash,
    extracted_at as updated_at
    
from {{ ref('stg_drivers') }}

{% endsnapshot %}

