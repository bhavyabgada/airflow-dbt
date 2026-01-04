/*
    Model: mart_reconciliation
    Layer: Mart
    Description: 
        Reconciliation report comparing trips to payments.
        Identifies discrepancies for financial close.
    
    Problems Addressed:
    - #3 Data Reconciliation: Trips vs Payments vs Payouts
    - #34 Circuit Breaker: Provides data for quality gates
*/

{{ config(
    materialized='table',
    tags=['mart', 'reconciliation', 'audit']
) }}

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

payments as (
    select
        payment_timestamp::date as payment_date,
        trip_id
    from {{ ref('int_payments_reconciled') }}
    where is_completed = true
      and is_refund = false
),

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

orphan_payments as (
    select
        payment_timestamp::date as payment_date,
        count(*) as orphan_count
    from {{ ref('int_payments_reconciled') }}
    where is_orphan_payment = true
    group by 1
),

reconciliation as (
    select
        coalesce(t.request_date, ps.payment_date) as reconciliation_date,
        coalesce(t.city_id, ps.city_id) as city_id,
        
        -- Trip side
        coalesce(t.trip_count, 0) as trips_completed,
        coalesce(t.trip_gross_usd, 0) as trips_gross_usd,
        coalesce(t.trip_driver_earnings_usd, 0) as trips_driver_earnings_usd,
        
        -- Payment side
        coalesce(ps.paid_trip_count, 0) as trips_paid,
        coalesce(ps.paid_gross_usd, 0) as payments_gross_usd,
        
        -- Orphans
        coalesce(op.orphan_count, 0) as orphan_payments,
        
        -- Reconciliation metrics
        coalesce(t.trip_count, 0) - coalesce(ps.paid_trip_count, 0) as unpaid_trips,
        coalesce(t.trip_gross_usd, 0) - coalesce(ps.paid_gross_usd, 0) as revenue_discrepancy_usd,
        
        -- Match rates
        case 
            when coalesce(t.trip_count, 0) > 0 
            then coalesce(ps.paid_trip_count, 0)::decimal / t.trip_count
            else null
        end as payment_match_rate,
        
        -- Status
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
    
    -- Circuit breaker flag
    case 
        when payment_match_rate < {{ var('min_reconciliation_match_rate', 0.99) }}
        then true
        else false
    end as circuit_breaker_triggered,
    
    -- Audit
    current_timestamp as _mart_loaded_at,
    '{{ invocation_id }}' as _invocation_id
    
from reconciliation

