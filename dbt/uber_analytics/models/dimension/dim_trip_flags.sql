/*
    Model: dim_trip_flags
    Layer: Dimension
    Description: 
        Junk dimension combining low-cardinality trip flags.
        Reduces fact table width and improves query performance.
    
    Problems Addressed:
    - #25 Junk Dimensions: Combine boolean flags
*/

{{ config(
    materialized='table',
    tags=['dimension', 'junk']
) }}

-- Generate all possible combinations of flags
with flag_combinations as (
    select
        is_surge::boolean as is_surge,
        is_pool::boolean as is_pool,
        is_premium::boolean as is_premium,
        has_tip::boolean as has_tip,
        has_promo::boolean as has_promo,
        is_late_arrival::boolean as is_late_arrival,
        is_completed::boolean as is_completed,
        is_cancelled::boolean as is_cancelled
    from (select true as flag union select false) is_surge
    cross join (select true as flag union select false) is_pool
    cross join (select true as flag union select false) is_premium
    cross join (select true as flag union select false) has_tip
    cross join (select true as flag union select false) has_promo
    cross join (select true as flag union select false) is_late_arrival
    cross join (select true as flag union select false) is_completed
    cross join (select true as flag union select false) is_cancelled
    -- Filter out impossible combinations
    where not (is_completed and is_cancelled)
)

select
    -- Junk dimension key
    {{ generate_junk_dim_key([
        'is_surge', 'is_pool', 'is_premium', 'has_tip', 
        'has_promo', 'is_late_arrival', 'is_completed', 'is_cancelled'
    ]) }} as trip_flag_key,
    
    -- Individual flags
    is_surge,
    is_pool,
    is_premium,
    has_tip,
    has_promo,
    is_late_arrival,
    is_completed,
    is_cancelled,
    
    -- Derived descriptions
    case 
        when is_surge then 'Surge Pricing'
        else 'Standard Pricing'
    end as pricing_type,
    
    case
        when is_completed then 'Completed'
        when is_cancelled then 'Cancelled'
        else 'Other'
    end as trip_outcome,
    
    -- Flag count (useful for filtering)
    (is_surge::int + is_pool::int + is_premium::int + 
     has_tip::int + has_promo::int + is_late_arrival::int) as special_flag_count,
    
    -- Audit
    current_timestamp as _loaded_at

from flag_combinations

