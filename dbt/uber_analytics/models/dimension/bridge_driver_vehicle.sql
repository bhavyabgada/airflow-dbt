/*
================================================================================
Model: bridge_driver_vehicle
Layer: Dimension
Type: Bridge Table
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#18 BRIDGE TABLES (Many-to-Many Relationships)
    Problem: Drivers and vehicles have a many-to-many relationship:
             - One driver can use MULTIPLE vehicles (owns Tesla, rents a Honda)
             - One vehicle can be used by MULTIPLE drivers (fleet car, rental)
    
             In dimensional modeling, fact tables should join to dimensions
             via single keys, not handle many-to-many directly.
    
    Example Data:
    | Driver | Vehicle | Usage |
    |--------|---------|-------|
    | D101   | V001    | 60%   |
    | D101   | V002    | 40%   |  <-- D101 uses two vehicles
    | D102   | V002    | 100%  |  <-- V002 is shared
    
    Business Impact: 
    - Can't correctly attribute trips to vehicles
    - Double-counting if joining both tables to fact
    - Complex SQL to handle relationships

    Solution: BRIDGE TABLE
    - Create intermediate table with compound key (driver_id + vehicle_id)
    - Include allocation weight for proportional analysis
    - Include validity period (effective_from, effective_to)
    - Use weighting factor to avoid double-counting
    
    Schema:
    ```sql
    driver_vehicle_key   -- Surrogate key
    driver_id           -- FK to dim_driver
    vehicle_id          -- FK to dim_vehicle (or stg_vehicles)
    allocation_weight   -- How much of driver's activity is on this vehicle
    effective_from      -- When association started
    effective_to        -- When association ended (NULL if current)
    is_primary_vehicle  -- Main vehicle for this driver
    ```

--------------------------------------------------------------------------------

WEIGHTING FACTOR:
    When aggregating metrics that span both driver and vehicle, use weights:
    
    Wrong (double counts):
    ```sql
    SELECT driver_id, vehicle_id, sum(trips) 
    FROM fct_trips
    GROUP BY driver_id, vehicle_id
    ```
    
    Correct (weighted):
    ```sql
    SELECT v.make, sum(t.trips * b.allocation_weight) as attributed_trips
    FROM fct_trips t
    JOIN bridge_driver_vehicle b ON t.driver_id = b.driver_id
    JOIN dim_vehicle v ON b.vehicle_id = v.vehicle_id
    GROUP BY v.make
    ```

--------------------------------------------------------------------------------

VALIDITY PERIODS:
    Drivers change vehicles over time. Without date ranges:
    - Join would return ALL historical vehicles
    - Can't determine which vehicle was used for a specific trip
    
    With effective_from/to:
    ```sql
    JOIN bridge_driver_vehicle b ON t.driver_id = b.driver_id
        AND t.trip_date >= b.effective_from
        AND (t.trip_date < b.effective_to OR b.effective_to IS NULL)
    ```

================================================================================
*/

{{ config(
    materialized='table',
    tags=['dimension', 'bridge']
) }}

with vehicles as (
    select * from {{ ref('stg_vehicles') }}
),

drivers as (
    select distinct 
        driver_id,
        signup_date
    from {{ ref('stg_drivers') }}
),

-- Build bridge table from vehicle data (driver_id is in vehicle records)
driver_vehicles as (
    select
        v.driver_id,
        v.vehicle_id,
        v.registration_date,
        v.is_active,
        
        -- Count vehicles per driver to calculate allocation weight
        count(*) over (partition by v.driver_id where v.is_active = true) as driver_active_vehicle_count
        
    from vehicles v
    where v.driver_id is not null
),

bridge as (
    select
        -- Compound surrogate key
        {{ generate_surrogate_key(['driver_id', 'vehicle_id']) }} as driver_vehicle_key,
        
        -- Foreign keys
        driver_id,
        vehicle_id,
        
        -- ALLOCATION WEIGHT: For proportional analysis
        -- If driver has 2 active vehicles, each gets 0.5 weight
        -- Used to avoid double-counting in aggregations
        case 
            when driver_active_vehicle_count > 0 
            then 1.0 / driver_active_vehicle_count
            else 1.0
        end as allocation_weight,
        
        -- Validity period (when was this association active?)
        registration_date as effective_from,
        case 
            when is_active = false then registration_date + interval '1 year'  -- Estimate
            else null  -- Still active
        end as effective_to,
        
        -- Primary vehicle flag (first registered vehicle)
        row_number() over (partition by driver_id order by registration_date) = 1 as is_primary_vehicle,
        
        -- Current status
        is_active as is_currently_active,
        
        -- Audit
        current_timestamp as _loaded_at
        
    from driver_vehicles
)

select * from bridge
