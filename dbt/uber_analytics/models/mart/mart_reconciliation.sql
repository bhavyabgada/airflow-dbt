/*
================================================================================
Model: mart_reconciliation
Layer: Mart
Purpose: Financial Reconciliation Report
================================================================================

PROBLEMS SOLVED:
--------------------------------------------------------------------------------

#3 DATA RECONCILIATION
    Problem: Multiple systems process the same transactions:
             - Trip System: Records trips and fares
             - Payment System: Processes charges and payouts
    
             These MUST match. If they don't:
             - Customers charged but no trip recorded (or vice versa)
             - Drivers not paid for completed trips
             - Revenue leakage or overstated revenue
    
    Business Impact: Financial misstatements, driver trust issues, audit failures.
    
    Solution:
    - Compare trip counts and amounts between systems DAILY
    - Calculate match rate (should be >= 99%)
    - Identify specific discrepancy types:
      - TRIPS_UNMATCHED: Trips without payments
      - PAYMENTS_UNMATCHED: Payments without trips
      - DISCREPANCY: Count/amount mismatch
      - RECONCILED: Everything matches
    
    Key Metrics:
    - trips_completed: Count from trip system
    - trips_paid: Count from payment system
    - payment_match_rate: trips_paid / trips_completed
    - revenue_discrepancy_usd: Amount difference

--------------------------------------------------------------------------------

#23 CIRCUIT BREAKER PATTERN
    Problem: If data quality is bad, downstream reports will be wrong.
             Better to STOP the pipeline than propagate bad data.
    
    Example: Match rate drops to 80% (usually 99.5%)
             - Don't update dashboards with incomplete data
             - Alert data team to investigate
             - Hold pipeline until issue resolved
    
    Business Impact: Prevents bad data from reaching executives/analysts.
    
    Solution:
    - Calculate `circuit_breaker_triggered` flag
    - If match_rate < threshold (99%), flag = true
    - Airflow DAG checks this flag before continuing
    - Pipeline branches to failure path if triggered
    
    Code in Airflow DAG:
    ```python
    def check_quality_gate(**context):
        # Query mart_reconciliation for circuit_breaker_triggered
        if circuit_breaker_triggered:
            return 'quality_failed'  # Stop pipeline
        return 'continue_pipeline'
    ```

--------------------------------------------------------------------------------

RECONCILIATION LOGIC:

1. GROUP trips by (date, city) → trips_completed, trips_gross_usd
2. GROUP payments by (date, city) → trips_paid, payments_gross_usd
3. FULL OUTER JOIN to catch orphans on either side
4. Calculate discrepancies and match rates
5. Flag any day with match_rate < threshold

================================================================================
*/

{{ config(
    materialized='table',
    tags=['mart', 'reconciliation', 'audit']
) }}

-- Aggregate completed trips by date and city
with trips as (
    select
        request_date,
        city_id,
        count(*) as trip_count,
        sum(total_fare_usd) as trip_gross_usd,
        sum(driver_earnings_usd) as trip_driver_earnings_usd
    from {{ ref('fct_trips') }}
    where trip_status = 'TRIP_COMPLETED'
    group by 1, 2
),

-- Get payments that are completed (not refunds)
payments as (
    select
        payment_timestamp::date as payment_date,
        trip_id
    from {{ ref('int_payments_reconciled') }}
    where is_completed = true
      and is_refund = false
),

-- Aggregate payments by date and city (via trip lookup)
payment_summary as (
    select
        p.payment_date,
        t.city_id,
        count(distinct p.trip_id) as paid_trip_count,
        sum(t.total_fare_usd) as paid_gross_usd
    from payments p
    inner join {{ ref('fct_trips') }} t on p.trip_id = t.trip_id
    group by 1, 2
),

-- Count orphan payments (payments without matching trips)
orphan_payments as (
    select
        payment_timestamp::date as payment_date,
        count(*) as orphan_count
    from {{ ref('int_payments_reconciled') }}
    where is_orphan_payment = true
    group by 1
),

-- PROBLEM #3: Full reconciliation comparison
reconciliation as (
    select
        coalesce(t.request_date, ps.payment_date) as reconciliation_date,
        coalesce(t.city_id, ps.city_id) as city_id,
        
        -- Trip system metrics
        coalesce(t.trip_count, 0) as trips_completed,
        coalesce(t.trip_gross_usd, 0) as trips_gross_usd,
        coalesce(t.trip_driver_earnings_usd, 0) as trips_driver_earnings_usd,
        
        -- Payment system metrics
        coalesce(ps.paid_trip_count, 0) as trips_paid,
        coalesce(ps.paid_gross_usd, 0) as payments_gross_usd,
        
        -- Orphan payments (require investigation)
        coalesce(op.orphan_count, 0) as orphan_payments,
        
        -- Discrepancy calculations
        coalesce(t.trip_count, 0) - coalesce(ps.paid_trip_count, 0) as unpaid_trips,
        coalesce(t.trip_gross_usd, 0) - coalesce(ps.paid_gross_usd, 0) as revenue_discrepancy_usd,
        
        -- Match rate (key metric for data quality)
        case 
            when coalesce(t.trip_count, 0) > 0 
            then coalesce(ps.paid_trip_count, 0)::decimal / t.trip_count
            else null
        end as payment_match_rate,
        
        -- Reconciliation status (human-readable)
        case
            when abs(coalesce(t.trip_count, 0) - coalesce(ps.paid_trip_count, 0)) <= 1 
                 and abs(coalesce(t.trip_gross_usd, 0) - coalesce(ps.paid_gross_usd, 0)) < 10
            then 'RECONCILED'
            when coalesce(t.trip_count, 0) > coalesce(ps.paid_trip_count, 0)
            then 'TRIPS_UNMATCHED'
            when coalesce(ps.paid_trip_count, 0) > coalesce(t.trip_count, 0)
            then 'PAYMENTS_UNMATCHED'
            else 'DISCREPANCY'
        end as reconciliation_status
        
    from trips t
    full outer join payment_summary ps 
        on t.request_date = ps.payment_date 
        and t.city_id = ps.city_id
    left join orphan_payments op on t.request_date = op.payment_date
)

select
    {{ generate_surrogate_key(['reconciliation_date', 'city_id']) }} as reconciliation_key,
    *,
    
    -- PROBLEM #23: Circuit breaker flag
    -- If match rate below threshold, trigger circuit breaker
    case 
        when payment_match_rate < {{ var('min_reconciliation_match_rate', 0.99) }}
        then true
        else false
    end as circuit_breaker_triggered,
    
    -- Audit
    current_timestamp as _mart_loaded_at,
    '{{ invocation_id }}' as _invocation_id
    
from reconciliation
