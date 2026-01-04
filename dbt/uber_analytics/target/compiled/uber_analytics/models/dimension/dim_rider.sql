/*
    Model: dim_rider
    Layer: Dimension
    Description: 
        Rider dimension built from SCD Type 2 snapshot.
    
    Problems Addressed:
    - #6 Point-in-Time Joins
    - #27 Data Retention/GDPR: Includes retention date
*/



with snapshot_data as (
    select * from "uber_analytics"."dimension"."snap_rider"
),

enriched as (
    select
        -- Surrogate key for this version
        
    md5(
        coalesce(cast(rider_id as varchar), '_NULL_') || '|' ||
        coalesce(cast(dbt_valid_from as varchar), '_NULL_')
    )
 as rider_key,
        
        -- Natural key
        rider_id,
        
        -- Attributes
        
        first_name_hash,
        last_name_hash,
        email_hash,
        phone_hash,
        
        
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
        
        -- GDPR retention
        data_retention_until,
        case 
            when data_retention_until < current_date then true 
            else false 
        end as is_eligible_for_deletion,
        
        -- SCD2 validity
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        case when dbt_valid_to is null then true else false end as is_current,
        
        -- Audit
        dbt_scd_id,
        dbt_updated_at,
        current_timestamp as _loaded_at
        
    from snapshot_data
)

select * from enriched