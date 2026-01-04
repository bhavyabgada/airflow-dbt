/*
================================================================================
Model: dim_trip_flags
Layer: Dimension
Type: Junk Dimension
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#19 JUNK DIMENSIONS
    Problem: Trip records have many low-cardinality boolean/flag columns:
             - is_surge (surge pricing active)
             - is_pool (shared ride)
             - is_premium (Black/SUV service)
             - has_tip (driver received tip)
             - has_promo (promo code used)
             - is_late_arrival (data arrived late)
             - is_completed (trip finished)
             - is_cancelled (trip cancelled)
    
             Storing 8 boolean columns in fact table:
             - Wastes space (8 columns × billions of rows)
             - Complicates queries (WHERE is_surge AND has_tip AND NOT is_cancelled)
             - Makes fact table wider and harder to maintain
    
    Business Impact: Higher storage costs, slower queries, complex SQL.
    
    Solution: JUNK DIMENSION
    - Pre-generate all valid flag combinations: 2^8 = 256 combinations
    - Filter out invalid ones (e.g., can't be both completed AND cancelled): ~192 valid
    - Store single `trip_flag_key` in fact table
    - Join to junk dimension for any flag-based analysis
    
    Benefits:
    - Fact table has 1 column instead of 8
    - All flag combinations pre-computed
    - Easy to add derived attributes (pricing_type, trip_outcome)
    - Queries become cleaner: JOIN dim_trip_flags WHERE is_surge = true
    
    Trade-off: Extra join, but dimension is tiny (192 rows, often cached)

--------------------------------------------------------------------------------

DESIGN DECISIONS:

1. Pre-generate ALL combinations vs Extract from facts
   - Chose pre-generate: Ensures consistent keys, no orphans
   - 192 rows is trivial, loads in milliseconds

2. MD5 hash key vs Integer surrogate
   - Chose MD5: Self-describing, deterministic from flag values
   - Same flags always produce same key (idempotent)

3. Added derived columns
   - pricing_type: 'Surge Pricing' or 'Standard Pricing'
   - trip_outcome: 'Completed', 'Cancelled', 'Other'
   - special_flag_count: Number of special flags (useful for filtering)

================================================================================
*/

{{ config(
    materialized='table',
    tags=['dimension', 'junk']
) }}

-- Generate all possible combinations of flags using CROSS JOIN
with bool_values as (
    select bool_val
    from (values (true), (false)) as t(bool_val)
),

-- Generate 2^8 = 256 combinations
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
    -- Filter out INVALID combinations
    -- A trip cannot be both completed AND cancelled
    where not (c.bool_val and cn.bool_val)
)

select
    -- Junk dimension key (MD5 hash of all flag values)
    -- Same flags always produce same key (deterministic)
    {{ generate_junk_dim_key([
        'is_surge', 'is_pool', 'is_premium', 'has_tip', 
        'has_promo', 'is_late_arrival', 'is_completed', 'is_cancelled'
    ]) }} as trip_flag_key,
    
    -- Individual flags (for filtering in queries)
    is_surge,
    is_pool,
    is_premium,
    has_tip,
    has_promo,
    is_late_arrival,
    is_completed,
    is_cancelled,
    
    -- DERIVED: Human-readable descriptions
    case 
        when is_surge then 'Surge Pricing'
        else 'Standard Pricing'
    end as pricing_type,
    
    case
        when is_completed then 'Completed'
        when is_cancelled then 'Cancelled'
        else 'Other'
    end as trip_outcome,
    
    -- DERIVED: Count of special flags (for filtering complex trips)
    (is_surge::int + is_pool::int + is_premium::int + 
     has_tip::int + has_promo::int + is_late_arrival::int) as special_flag_count,
    
    -- Audit
    current_timestamp as _loaded_at

from flag_combinations
