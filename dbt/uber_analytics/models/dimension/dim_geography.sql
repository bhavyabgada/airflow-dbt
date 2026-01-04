/*
================================================================================
Model: dim_geography
Layer: Dimension
Type: Hierarchy Dimension
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#9 HIERARCHICAL DATA (Geographic Drill-Down)
    Problem: Geographic data has natural hierarchies for reporting:
             - Zone → City → Country → Region → Global
    
             Business users need to:
             - View global totals
             - Drill down: "Why is EMEA revenue down?"
             - Drill further: "Which country in EMEA?"
             - Keep drilling: "Which city in UK?"
             - Final level: "Which zone in London?"
    
             Without proper hierarchy, every query needs complex GROUP BY
             or multiple CTEs to roll up data.
    
    Example Hierarchy:
    ```
    Global
    └── AMER (Region)
        └── US (Country)
            └── New York (City)
                ├── Manhattan (Zone)
                ├── Brooklyn (Zone)
                └── Queens (Zone)
            └── Los Angeles (City)
                ├── Downtown (Zone)
                └── Hollywood (Zone)
    └── EMEA (Region)
        └── UK (Country)
            └── London (City)
                ├── Heathrow (Zone)
                ├── City Center (Zone)
                └── Canary Wharf (Zone)
    ```
    
    Business Impact: Cannot do proper geographic drill-through analysis.

--------------------------------------------------------------------------------

SOLUTION: HIERARCHY DIMENSION
    
    1. Denormalize hierarchy into single dimension table:
       | zone_id | zone | city | country | region |
       | LON-01  | Heathrow | London | UK | EMEA |
       | LON-02  | City Center | London | UK | EMEA |
    
    2. Include keys at EACH level for flexible aggregation:
       - geography_key (zone level - finest grain)
       - city_key
       - country_key  
       - region_key
    
    3. Pre-calculate drill path for BI tools:
       ```sql
       concat(region, ' > ', country, ' > ', city, ' > ', zone_name) as geo_path
       ```

--------------------------------------------------------------------------------

USAGE PATTERNS:
    
    -- Group by city
    SELECT g.city, sum(f.revenue)
    FROM fct_trips f
    JOIN dim_geography g ON f.geography_key = g.geography_key
    GROUP BY g.city
    
    -- Filter by region, group by city
    SELECT g.city, sum(f.revenue)
    FROM fct_trips f  
    JOIN dim_geography g ON f.geography_key = g.geography_key
    WHERE g.region = 'EMEA'
    GROUP BY g.city
    
    -- Full drill-down
    SELECT g.region, g.country, g.city, g.zone_name, sum(f.revenue)
    FROM fct_trips f
    JOIN dim_geography g ON f.geography_key = g.geography_key
    GROUP BY ROLLUP(g.region, g.country, g.city, g.zone_name)

================================================================================
*/

{{ config(
    materialized='table',
    tags=['dimension', 'geography']
) }}

with cities as (
    select * from {{ ref('ref_cities') }}
),

-- Define zones within cities (for finest grain geography)
-- In production, this would come from an operational zones table
zones as (
    select 
        city_id,
        city_id || '-DOWNTOWN' as zone_id,
        'Downtown' as zone_name,
        'CBD' as zone_type
    from cities
    
    union all
    
    select 
        city_id,
        city_id || '-AIRPORT' as zone_id,
        'Airport' as zone_name,
        'AIRPORT' as zone_type
    from cities
    
    union all
    
    select 
        city_id,
        city_id || '-SUBURBAN' as zone_id,
        'Suburban' as zone_name,
        'RESIDENTIAL' as zone_type
    from cities
),

-- Build denormalized hierarchy
geography as (
    select
        -- ZONE LEVEL (finest grain)
        {{ generate_surrogate_key(['z.zone_id']) }} as geography_key,
        z.zone_id,
        z.zone_name,
        z.zone_type,
        
        -- CITY LEVEL
        {{ generate_surrogate_key(['c.city_id']) }} as city_key,
        c.city_id,
        c.city_name,
        c.timezone,
        c.currency_code,
        c.fiscal_calendar_type,
        
        -- COUNTRY LEVEL (derived from city_id pattern)
        case 
            when c.city_id in ('NYC', 'LAX', 'CHI', 'MIA', 'SFO') then 'US'
            when c.city_id = 'LON' then 'UK'
            when c.city_id = 'PAR' then 'FR'
            when c.city_id = 'TYO' then 'JP'
            when c.city_id = 'SYD' then 'AU'
            when c.city_id = 'MEX' then 'MX'
            when c.city_id = 'SAO' then 'BR'
            when c.city_id = 'TOR' then 'CA'
            when c.city_id = 'BER' then 'DE'
            when c.city_id = 'SIN' then 'SG'
            else 'OTHER'
        end as country_code,
        
        case 
            when c.city_id in ('NYC', 'LAX', 'CHI', 'MIA', 'SFO') then 'United States'
            when c.city_id = 'LON' then 'United Kingdom'
            when c.city_id = 'PAR' then 'France'
            when c.city_id = 'TYO' then 'Japan'
            when c.city_id = 'SYD' then 'Australia'
            when c.city_id = 'MEX' then 'Mexico'
            when c.city_id = 'SAO' then 'Brazil'
            when c.city_id = 'TOR' then 'Canada'
            when c.city_id = 'BER' then 'Germany'
            when c.city_id = 'SIN' then 'Singapore'
            else 'Other'
        end as country_name,
        
        -- REGION LEVEL
        case 
            when c.city_id in ('NYC', 'LAX', 'CHI', 'MIA', 'SFO', 'MEX', 'SAO', 'TOR') then 'AMER'
            when c.city_id in ('LON', 'PAR', 'BER') then 'EMEA'
            when c.city_id in ('TYO', 'SYD', 'SIN') then 'APAC'
            else 'OTHER'
        end as region_code,
        
        case 
            when c.city_id in ('NYC', 'LAX', 'CHI', 'MIA', 'SFO', 'MEX', 'SAO', 'TOR') then 'Americas'
            when c.city_id in ('LON', 'PAR', 'BER') then 'Europe, Middle East & Africa'
            when c.city_id in ('TYO', 'SYD', 'SIN') then 'Asia Pacific'
            else 'Other'
        end as region_name,
        
        -- DRILL PATH: For BI tool breadcrumbs
        concat(
            case when c.city_id in ('NYC', 'LAX', 'CHI', 'MIA', 'SFO', 'MEX', 'SAO', 'TOR') then 'AMER'
                 when c.city_id in ('LON', 'PAR', 'BER') then 'EMEA'
                 when c.city_id in ('TYO', 'SYD', 'SIN') then 'APAC'
                 else 'OTHER' end,
            ' > ',
            c.city_name,
            ' > ',
            z.zone_name
        ) as geo_drill_path,
        
        -- Useful flags
        c.is_major_market,
        z.zone_type = 'AIRPORT' as is_airport_zone,
        z.zone_type = 'CBD' as is_downtown_zone,
        
        -- Audit
        current_timestamp as _loaded_at
        
    from zones z
    inner join cities c on z.city_id = c.city_id
)

select * from geography
