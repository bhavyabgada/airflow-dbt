/*
================================================================================
Model: stg_drivers
Layer: Staging
Source: Driver App / HR System
================================================================================

PROBLEMS SOLVED:
--------------------------------------------------------------------------------

#15 PII HANDLING (Personally Identifiable Information)
    Problem: Driver data contains sensitive PII (SSN, email, phone, name).
             - Dev/Test environments shouldn't have real PII
             - GDPR/CCPA require data minimization
             - Security audits flag raw PII in data warehouses
    
    Business Impact: Compliance violations, security risks, audit failures.
    
    Solution:
    - Use dbt variable `mask_pii` to conditionally hash/mask fields
    - Dev: mask_pii = true → MD5 hashes and masked values
    - Prod: mask_pii = false → Original values (with proper access controls)
    - Pattern:
      ```sql
      {% if var('mask_pii', true) %}
          md5(email) as email_hash,
      {% else %}
          email,
      {% endif %}
      ```
    
    Configuration in dbt_project.yml:
    ```yaml
    vars:
      mask_pii: true  # Set to false in prod profile
    ```

--------------------------------------------------------------------------------

#11 SCD TYPE 2 PREPARATION (Change Detection Hash)
    Problem: Need to detect when driver attributes change to create new
             SCD Type 2 versions. Comparing all columns is inefficient.
    
    Business Impact: Missed historical changes, bloated snapshot tables.
    
    Solution:
    - Create `_row_hash` from columns we want to track
    - Snapshot compares hash instead of individual columns
    - Only tracked columns trigger new version:
      - status, rating, total_trips, city_id, is_active
    - Static columns (name, DOB, signup_date) don't trigger new versions
    
    Code Pattern:
    ```sql
    md5(
        coalesce(status, '') || '|' ||
        coalesce(rating::varchar, '') || '|' ||
        coalesce(city_id, '') || '|' ||
        coalesce(is_active::varchar, '')
    ) as _row_hash
    ```

================================================================================
*/

{{ config(
    materialized='view',
    tags=['staging', 'drivers']
) }}

with source_data as (
    select * from {{ ref('source_drivers') }}
),

standardized as (
    select
        -- Driver identifiers
        driver_id,
        
        -- PROBLEM #15: PII fields (conditionally masked based on environment)
        {% if var('mask_pii', true) %}
        -- Development/Test: Hash PII for privacy
        md5(first_name) as first_name_hash,
        md5(last_name) as last_name_hash,
        md5(email) as email_hash,
        md5(phone) as phone_hash,
        '****' as ssn_masked,  -- Always mask SSN, never expose
        {% else %}
        -- Production: Keep original values (with proper access controls)
        first_name,
        last_name,
        email,
        phone,
        ssn_last_four,
        {% endif %}
        
        date_of_birth::date as date_of_birth,
        
        -- License information (not PII but sensitive)
        license_number,
        license_state,
        license_expiry::date as license_expiry,
        
        -- Location
        city_id,
        
        -- Status fields (these change frequently - tracked in SCD2)
        status as driver_status,
        rating::decimal(3,2) as current_rating,
        total_trips::integer as total_trips,
        
        -- Dates
        signup_date::date as signup_date,
        last_active_date::date as last_active_date,
        
        -- Background check
        background_check_status,
        background_check_date::date as background_check_date,
        
        -- Active flag
        is_active::boolean as is_active,
        
        -- PROBLEM #11: Create hash for SCD2 change detection
        -- Only include columns that should trigger a new version
        -- Static columns (name, DOB, signup_date) are excluded
        md5(
            coalesce(status, '') || '|' ||
            coalesce(rating::varchar, '') || '|' ||
            coalesce(total_trips::varchar, '') || '|' ||
            coalesce(city_id, '') || '|' ||
            coalesce(is_active::varchar, '')
        ) as _row_hash,
        
        -- Metadata
        source_system,
        extracted_at::timestamp as extracted_at,
        
        -- Audit columns
        current_timestamp as _stg_loaded_at,
        '{{ invocation_id }}' as _stg_invocation_id
        
    from source_data
)

select * from standardized
