
  
    

  create  table "uber_analytics"."dev_dimension"."dim_geography__dbt_tmp"
  
  
    as
  
  (
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



with cities as (
    select * from "uber_analytics"."dev_staging"."ref_cities"
),

surge_zones as (
    -- Get distinct zones from surge data
    select distinct
        city_id,
        zone_id,
        zone_name
    from "uber_analytics"."dev_staging"."stg_surge_snapshots"
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
    
    md5(
        
            coalesce(cast(zone_id as varchar), '_NULL_')
            
        
    )
 as geography_key,
    *,
    -- Audit
    current_timestamp as _loaded_at,
    '2a9b2b56-d1bf-499d-92f6-dd6e735dddce' as _invocation_id
from geography
  );
  