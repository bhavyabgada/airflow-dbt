/*
    Model: bridge_driver_vehicle
    Layer: Dimension
    Description: 
        Bridge table for many-to-many driver-vehicle relationship.
        A driver can have multiple vehicles, each vehicle at different times.
    
    Problems Addressed:
    - #21 Bridge Tables: Driver ↔ Vehicle many-to-many
*/



with vehicles as (
    select * from "uber_analytics"."dev_staging"."stg_vehicles"
),

bridge as (
    select
        -- Bridge key
        
    md5(
        
            coalesce(cast(driver_id as varchar), '_NULL_')
             || '|' || 
        
            coalesce(cast(vehicle_id as varchar), '_NULL_')
            
        
    )
 as bridge_key,
        
        -- Keys
        driver_id,
        vehicle_id,
        
        -- Relationship attributes
        is_primary_vehicle,
        
        -- Vehicle attributes (denormalized for convenience)
        make,
        model,
        year,
        color,
        license_plate,
        vehicle_type,
        passenger_capacity,
        
        -- Compliance
        registration_expiry,
        insurance_expiry,
        last_inspection_date,
        days_until_registration_expiry,
        
        -- Status
        is_active,
        
        -- For weighting in aggregations
        case when is_primary_vehicle then 1.0 else 0.5 end as allocation_weight,
        
        -- Audit
        extracted_at,
        current_timestamp as _loaded_at,
        '402658b7-059e-4de5-83c3-55909b7e2657' as _invocation_id
        
    from vehicles
)

select * from bridge