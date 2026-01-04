/*
    Model: dim_geography
    Layer: Dimension
    Description: 
        Geography dimension with hierarchical structure.
        Zone → City → Country → Region hierarchy.
    
    Problems Addressed:
    - #9 Hierarchical Data: Zone → City → Country → Region
    - #22 Outrigger Dimensions: City links to country details
*/

{{ config(
    materialized='table',
    tags=['dimension', 'geography']
) }}

with cities as (
    select * from {{ ref('ref_cities') }}
),

surge_zones as (
    -- Get distinct zones from surge data
    select distinct
        city_id,
        zone_id,
        zone_name
    from {{ ref('stg_surge_snapshots') }}
),

-- Build geography hierarchy
geography as (
    select
        -- Zone level (most granular)
        coalesce(z.zone_id, c.city_id || '_DEFAULT') as zone_id,
        coalesce(z.zone_name, c.city_name || ' - Default Zone') as zone_name,
        
        -- City level
        c.city_id,
        c.city_name,
        c.timezone,
        c.currency_code,
        
        -- Country level
        c.country_code,
        c.country_name,
        
        -- Region level (highest)
        c.region,
        
        -- Fiscal calendar info
        c.fiscal_year_start_month,
        
        -- Status
        c.is_active,
        
        -- Hierarchy path (for tree navigation)
        c.region || ' > ' || c.country_name || ' > ' || c.city_name || ' > ' || 
            coalesce(z.zone_name, 'Default') as hierarchy_path,
        
        -- Hierarchy level
        4 as hierarchy_level,  -- Zone is level 4
        
        -- Parent keys (for drill-up)
        c.city_id as parent_city_id,
        c.country_code as parent_country_code,
        c.region as parent_region
        
    from cities c
    left join surge_zones z on c.city_id = z.city_id
)

select
    -- Surrogate key
    {{ generate_surrogate_key(['zone_id']) }} as geography_key,
    *,
    -- Audit
    current_timestamp as _loaded_at,
    '{{ invocation_id }}' as _invocation_id
from geography

