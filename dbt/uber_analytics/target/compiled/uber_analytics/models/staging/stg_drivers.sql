/*
    Model: stg_drivers
    Source: Driver App / HR System
    Description: Staging model for driver profile data.
                 Prepares data for SCD Type 2 dimension.
    
    Problems Addressed:
    - #16 PII Handling: Masks SSN, email, phone in non-prod
    - #11 Slowly vs Rapidly Changing: Separates static vs dynamic attributes
*/



with source_data as (
    select * from "uber_analytics"."dev_staging"."source_drivers"
),

standardized as (
    select
        -- Driver identifiers
        driver_id,
        
        -- PII fields (conditionally masked)
        
        md5(first_name) as first_name_hash,
        md5(last_name) as last_name_hash,
        md5(email) as email_hash,
        md5(phone) as phone_hash,
        '****' as ssn_masked,
        
        
        date_of_birth::date as date_of_birth,
        
        -- License information
        license_number,
        license_state,
        license_expiry::date as license_expiry,
        
        -- Location
        city_id,
        
        -- Status (rapidly changing - will go to mini dimension)
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
        
        -- Create a hash for change detection (non-timestamp incremental)
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
        '402658b7-059e-4de5-83c3-55909b7e2657' as _stg_invocation_id
        
    from source_data
)

select * from standardized