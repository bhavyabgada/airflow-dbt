/*
    Model: stg_vehicles
    Source: Driver App
    Description: Staging model for vehicle data.
                 Supports many-to-many driver-vehicle relationship (bridge table).
    
    Problems Addressed:
    - #21 Bridge Tables: Driver can have multiple vehicles
*/



with source_data as (
    select * from "uber_analytics"."dev_staging"."source_vehicles"
),

standardized as (
    select
        -- Identifiers
        vehicle_id,
        driver_id,
        
        -- Vehicle details
        make,
        model,
        year::integer as year,
        color,
        license_plate,
        vehicle_type,
        capacity::integer as passenger_capacity,
        
        -- Relationship
        is_primary::boolean as is_primary_vehicle,
        
        -- Compliance dates
        registration_expiry::date as registration_expiry,
        insurance_expiry::date as insurance_expiry,
        inspection_date::date as last_inspection_date,
        
        -- Status
        is_active::boolean as is_active,
        
        -- Derived: Days until registration expires
        registration_expiry::date - current_date as days_until_registration_expiry,
        
        -- Metadata
        source_system,
        extracted_at::timestamp as extracted_at,
        
        -- Audit columns
        current_timestamp as _stg_loaded_at,
        '402658b7-059e-4de5-83c3-55909b7e2657' as _stg_invocation_id
        
    from source_data
)

select * from standardized