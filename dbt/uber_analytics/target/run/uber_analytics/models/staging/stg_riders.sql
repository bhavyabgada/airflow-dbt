
  create view "uber_analytics"."dev_staging"."stg_riders__dbt_tmp"
    
    
  as (
    /*
    Model: stg_riders
    Source: Rider App
    Description: Staging model for rider profile data.
    
    Problems Addressed:
    - #16 PII Handling: Masks personal information
    - #27 Data Retention/GDPR: Adds retention metadata
*/



with source_data as (
    select * from "uber_analytics"."dev_staging"."source_riders"
),

standardized as (
    select
        -- Rider identifiers
        rider_id,
        
        -- PII fields (conditionally masked)
        
        md5(first_name) as first_name_hash,
        md5(last_name) as last_name_hash,
        md5(email) as email_hash,
        md5(phone) as phone_hash,
        
        
        date_of_birth::date as date_of_birth,
        
        -- Location
        city_id,
        
        -- Loyalty tier
        rider_tier,
        
        -- Lifetime metrics
        lifetime_trips::integer as lifetime_trips,
        lifetime_spend_usd::decimal(12,2) as lifetime_spend_usd,
        
        -- Dates
        signup_date::date as signup_date,
        last_trip_date::date as last_trip_date,
        
        -- Payment
        preferred_payment_method,
        
        -- Flags
        is_active::boolean as is_active,
        fraud_flag::boolean as fraud_flag,
        
        -- GDPR: Calculate data retention eligibility
        -- Data can be deleted 7 years after last activity
        (last_trip_date::date + interval '7 years')::date as data_retention_until,
        
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