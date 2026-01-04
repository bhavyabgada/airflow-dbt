/*
    Model: fct_surge_snapshot
    Layer: Fact
    Description: 
        Periodic snapshot of surge pricing by zone.
        Captured every 2 minutes for point-in-time surge lookups.
    
    Problems Addressed:
    - #6 Point-in-Time Joins: Surge at trip request time
    - #11 Rapidly Changing: Surge changes every 2 minutes
*/



with surge_data as (
    select * from "uber_analytics"."dev_staging"."stg_surge_snapshots"
),

snapshot as (
    select
        -- Surrogate key
        
    md5(
        
            coalesce(cast(city_id as varchar), '_NULL_')
             || '|' || 
        
            coalesce(cast(zone_id as varchar), '_NULL_')
             || '|' || 
        
            coalesce(cast(snapshot_timestamp as varchar), '_NULL_')
            
        
    )
 as snapshot_key,
        
        -- Natural keys
        snapshot_id,
        city_id,
        zone_id,
        zone_name,
        
        -- Date key
        to_char(snapshot_timestamp::date, 'YYYYMMDD')::integer as date_key,
        
        -- Timestamp
        snapshot_timestamp,
        valid_from,
        valid_to,
        
        -- Surge measures
        surge_multiplier,
        
        -- Supply/demand indicators
        demand_level,
        supply_level,
        
        -- Derived measures
        case 
            when surge_multiplier >= 2.0 then 'CRITICAL'
            when surge_multiplier >= 1.5 then 'HIGH'
            when surge_multiplier >= 1.2 then 'MODERATE'
            else 'NORMAL'
        end as surge_level,
        
        -- For aggregation
        case when surge_multiplier > 1.0 then 1 else 0 end as is_surge_active,
        
        -- Hour of day (for pattern analysis)
        extract(hour from snapshot_timestamp)::integer as hour_of_day,
        extract(dow from snapshot_timestamp)::integer as day_of_week,
        
        -- Audit
        extracted_at,
        current_timestamp as _fact_loaded_at,
        '402658b7-059e-4de5-83c3-55909b7e2657' as _invocation_id
        
    from surge_data
)

select * from snapshot


    where snapshot_timestamp > (select max(snapshot_timestamp) from "uber_analytics"."dev_fact"."fct_surge_snapshot")
