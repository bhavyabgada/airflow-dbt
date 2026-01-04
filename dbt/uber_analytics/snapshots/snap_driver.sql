/*
================================================================================
Snapshot: snap_driver
Type: SCD Type 2
Strategy: Check (column-based change detection)
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#11 SCD TYPE 2 (Slowly Changing Dimension)
    Problem: Driver attributes change over time, but we need to preserve history:
             - Driver rating changes after each trip
             - Driver status changes (active → suspended → reactivated)
             - Driver moves to different city
             - Total trip count increases
    
             Without history:
             - Can't answer "What was driver's rating when this trip happened?"
             - Performance trends are lost
             - Compliance audits can't verify historical state
    
    Business Impact: Cannot do historical analysis, compliance failures.
    
    Solution: SLOWLY CHANGING DIMENSION TYPE 2
    - Each change creates a NEW ROW (don't update existing)
    - Add validity columns: dbt_valid_from, dbt_valid_to
    - NULL in dbt_valid_to means "currently active version"
    - Use dbt snapshots with 'check' strategy
    
    Before Change (driver D101):
    | driver_key | driver_id | rating | valid_from | valid_to   |
    |------------|-----------|--------|------------|------------|
    | abc123     | D101      | 4.5    | 2024-01-01 | NULL       |
    
    After Rating Change:
    | driver_key | driver_id | rating | valid_from | valid_to   |
    |------------|-----------|--------|------------|------------|
    | abc123     | D101      | 4.5    | 2024-01-01 | 2024-03-15 | <- Closed
    | def456     | D101      | 4.8    | 2024-03-15 | NULL       | <- New

--------------------------------------------------------------------------------

CHECK STRATEGY vs TIMESTAMP STRATEGY:
    
    Check Strategy (used here):
    - Compares specific columns: ['driver_status', 'current_rating', 'city_id']
    - Creates new version when ANY checked column changes
    - Good for: Tracking business-meaningful changes
    
    Timestamp Strategy:
    - Uses updated_at column from source
    - Creates new version when timestamp changes
    - Good for: Source systems that track their own updates

--------------------------------------------------------------------------------

USAGE IN DOWNSTREAM MODELS:
    
    Point-in-time lookup in fct_trips:
    ```sql
    SELECT t.*, d.rating_at_time_of_trip
    FROM trips t
    LEFT JOIN snap_driver d ON t.driver_id = d.driver_id
        AND t.trip_date >= d.dbt_valid_from
        AND (t.trip_date < d.dbt_valid_to OR d.dbt_valid_to IS NULL)
    ```

================================================================================
*/

{% snapshot snap_driver %}

{{
    config(
        target_schema='dimension',
        unique_key='driver_id',
        strategy='check',
        check_cols=['driver_status', 'current_rating', 'total_trips', 'city_id', 'is_active'],
        invalidate_hard_deletes=True
    )
}}

-- Select from staging model (not raw source)
-- This ensures standardization and cleansing is applied before snapshotting
select
    driver_id,
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
    source_system,
    extracted_at,
    _stg_loaded_at
from {{ ref('stg_drivers') }}

{% endsnapshot %}
