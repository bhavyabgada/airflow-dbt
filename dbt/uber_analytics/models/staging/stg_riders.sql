/*
================================================================================
Model: stg_riders
Layer: Staging
Source: Rider App
================================================================================

PROBLEMS SOLVED:
--------------------------------------------------------------------------------

#15 PII HANDLING (Personally Identifiable Information)
    Problem: Rider data contains highly sensitive PII:
             - Full name, email, phone (standard PII)
             - Payment method details
             - Trip history (behavioral data under GDPR)
    
             GDPR, CCPA, and other regulations require:
             - Data minimization (don't collect what you don't need)
             - Purpose limitation (only use data for stated purpose)
             - Right to deletion (riders can request data removal)
    
    Business Impact: Regulatory fines (up to 4% of global revenue under GDPR).
    
    Solution:
    - PII masking in non-production environments
    - Track `account_deletion_requested_at` timestamp
    - Calculate data retention eligibility

--------------------------------------------------------------------------------

#21 DATA RETENTION / GDPR COMPLIANCE
    Problem: GDPR Article 17 - "Right to be Forgotten"
             Riders can request their data be deleted.
             But we need to retain financial data for:
             - Tax purposes (7 years in most jurisdictions)
             - Legal disputes (statute of limitations)
             - Financial audits
    
    Business Impact: Must balance deletion rights vs legal retention requirements.
    
    Solution: RETENTION DATE CALCULATION
    - Track when rider requested deletion
    - Calculate earliest date we CAN delete (after legal hold expires)
    - Store both in dimension for compliance reporting
    
    Formula:
    ```sql
    -- 7 years from last financial transaction
    last_trip_date + interval '7 years' as data_retention_until
    
    -- If deletion requested, can delete when retention expires
    GREATEST(deletion_requested_at, data_retention_until) as eligible_for_deletion_at
    ```
    
    Compliance Query:
    ```sql
    SELECT * FROM dim_rider 
    WHERE account_deletion_requested = true 
      AND eligible_for_deletion_at <= current_date
    -- These riders are safe to anonymize
    ```

--------------------------------------------------------------------------------

RIDER TIER CALCULATION:
    Based on trip count and membership status:
    - PLATINUM: 100+ trips or Premium member
    - GOLD: 50+ trips
    - SILVER: 20+ trips
    - BRONZE: Default
    
    Used for prioritized support, promotions, surge pricing protection.

================================================================================
*/

{{ config(
    materialized='view',
    tags=['staging', 'riders']
) }}

with source_data as (
    select * from {{ ref('source_riders') }}
),

standardized as (
    select
        -- Rider identifiers
        rider_id,
        
        -- PROBLEM #15: PII fields (conditionally masked)
        {% if var('mask_pii', true) %}
        md5(first_name) as first_name_hash,
        md5(last_name) as last_name_hash,
        md5(email) as email_hash,
        md5(phone) as phone_hash,
        {% else %}
        first_name,
        last_name,
        email,
        phone,
        {% endif %}
        
        -- Payment info (masked even in prod for security)
        'xxxx-' || right(payment_method_last_four, 4) as payment_method_masked,
        default_payment_type,
        
        -- Location
        city_id,
        home_address_lat::decimal(10,6) as home_latitude,
        home_address_long::decimal(10,6) as home_longitude,
        
        -- Membership & tier
        is_premium_member::boolean as is_premium_member,
        
        -- Calculate rider tier based on activity
        case
            when is_premium_member::boolean then 'PLATINUM'
            when total_trips::integer >= 100 then 'PLATINUM'
            when total_trips::integer >= 50 then 'GOLD'
            when total_trips::integer >= 20 then 'SILVER'
            else 'BRONZE'
        end as rider_tier,
        
        -- Activity metrics
        total_trips::integer as total_trips,
        total_spend::decimal(12,2) as total_spend_local,
        currency_code,
        signup_date::date as signup_date,
        last_trip_date::date as last_trip_date,
        
        -- Account status
        account_status,
        is_active::boolean as is_active,
        
        -- PROBLEM #21: GDPR / Deletion tracking
        account_deletion_requested::boolean as account_deletion_requested,
        account_deletion_requested_at::timestamp as deletion_requested_at,
        
        -- Calculate data retention eligibility (7 years from last trip)
        case 
            when last_trip_date is not null 
            then last_trip_date::date + interval '7 years'
            else signup_date::date + interval '7 years'
        end as data_retention_until,
        
        -- Earliest date we can delete this rider's data
        case 
            when account_deletion_requested::boolean = true then
                greatest(
                    account_deletion_requested_at::timestamp,
                    (coalesce(last_trip_date::date, signup_date::date) + interval '7 years')::timestamp
                )
            else null
        end as eligible_for_deletion_at,
        
        -- Hash for SCD2 change detection (tracked columns only)
        md5(
            coalesce(account_status, '') || '|' ||
            coalesce(total_trips::varchar, '') || '|' ||
            coalesce(city_id, '') || '|' ||
            coalesce(is_premium_member::varchar, '') || '|' ||
            coalesce(is_active::varchar, '')
        ) as _row_hash,
        
        -- Metadata
        source_system,
        extracted_at::timestamp as extracted_at,
        
        -- Audit
        current_timestamp as _stg_loaded_at,
        '{{ invocation_id }}' as _stg_invocation_id
        
    from source_data
)

select * from standardized
