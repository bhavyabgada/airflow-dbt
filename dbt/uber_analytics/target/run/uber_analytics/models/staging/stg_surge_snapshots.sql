
  create view "uber_analytics"."dev_staging"."stg_surge_snapshots__dbt_tmp"
    
    
  as (
    /*
    Model: stg_surge_snapshots
    Source: Surge Pricing Service
    Description: Staging model for surge pricing snapshots.
                 Captured every 2 minutes per zone.
    
    Problems Addressed:
    - #6 Point-in-Time Joins: Surge at trip request time
    - #11 Rapidly Changing: Surge changes every 2 minutes
*/



with source_data as (
    select * from "uber_analytics"."dev_staging"."source_surge_snapshots"
),

standardized as (
    select
        -- Identifiers
        snapshot_id,
        city_id,
        zone_id,
        zone_name,
        
        -- Surge data
        surge_multiplier::decimal(4,2) as surge_multiplier,
        demand_level,
        supply_level,
        
        -- Timestamp
        snapshot_timestamp::timestamp as snapshot_timestamp,
        
        -- Derived: Snapshot validity window (2 minutes)
        snapshot_timestamp::timestamp as valid_from,
        (snapshot_timestamp::timestamp + interval '2 minutes') as valid_to,
        
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