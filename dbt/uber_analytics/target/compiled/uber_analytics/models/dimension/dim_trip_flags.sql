/*
    Model: dim_trip_flags
    Layer: Dimension
    Description: 
        Junk dimension combining low-cardinality trip flags.
        Reduces fact table width and improves query performance.
    
    Problems Addressed:
    - #25 Junk Dimensions: Combine boolean flags
*/



-- Generate all possible combinations of flags using generate_series
with bool_values as (
    select bool_val
    from (values (true), (false)) as t(bool_val)
),

flag_combinations as (
    select
        s.bool_val as is_surge,
        p.bool_val as is_pool,
        pr.bool_val as is_premium,
        t.bool_val as has_tip,
        pm.bool_val as has_promo,
        la.bool_val as is_late_arrival,
        c.bool_val as is_completed,
        cn.bool_val as is_cancelled
    from bool_values s
    cross join bool_values p
    cross join bool_values pr
    cross join bool_values t
    cross join bool_values pm
    cross join bool_values la
    cross join bool_values c
    cross join bool_values cn
    -- Filter out impossible combinations
    where not (c.bool_val and cn.bool_val)
)

select
    -- Junk dimension key
    
    md5(
        
            coalesce(cast(is_surge as varchar), 'N')
             || 
        
            coalesce(cast(is_pool as varchar), 'N')
             || 
        
            coalesce(cast(is_premium as varchar), 'N')
             || 
        
            coalesce(cast(has_tip as varchar), 'N')
             || 
        
            coalesce(cast(has_promo as varchar), 'N')
             || 
        
            coalesce(cast(is_late_arrival as varchar), 'N')
             || 
        
            coalesce(cast(is_completed as varchar), 'N')
             || 
        
            coalesce(cast(is_cancelled as varchar), 'N')
            
        
    )
 as trip_flag_key,
    
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