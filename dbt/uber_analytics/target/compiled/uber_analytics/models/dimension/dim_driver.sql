/*
    Model: dim_driver
    Layer: Dimension
    Description: 
        Driver dimension built from SCD Type 2 snapshot.
        Supports point-in-time lookups using valid_from/valid_to.
    
    Problems Addressed:
    - #6 Point-in-Time Joins
    - #11 SCD Type 2
*/



with snapshot_data as (
    select * from "uber_analytics"."dimension"."snap_driver"
),

enriched as (
    select
        -- Surrogate key for this version
        
    md5(
        coalesce(cast(driver_id as varchar), '_NULL_') || '|' ||
        coalesce(cast(dbt_valid_from as varchar), '_NULL_')
    )
 as driver_key,
        
        -- Natural key
        driver_id,
        
        -- Attributes (from snapshot)
        
        first_name_hash,
        last_name_hash,
        email_hash,
        phone_hash,
        ssn_masked,
        
        
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
        
        -- SCD2 validity
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        
        -- Current record flag
        case 
            when dbt_valid_to is null then true 
            else false 
        end as is_current,
        
        -- Row hash for change detection
        _row_hash,
        
        -- Audit
        dbt_scd_id,
        dbt_updated_at,
        current_timestamp as _loaded_at
        
    from snapshot_data
)

select * from enriched