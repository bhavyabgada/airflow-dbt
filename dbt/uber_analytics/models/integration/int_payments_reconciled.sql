/*
    Model: int_payments_reconciled
    Layer: Integration
    Description: 
        Payment data reconciled with trips.
        Identifies orphan payments and calculates net amounts.
    
    Problems Addressed:
    - #2 Orphan Records: Flags payments without matching trips
    - #3 Data Reconciliation: Matches payments to trips
    - #14 Idempotency: Uses payment_id as idempotency key
*/

{{ config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy='merge',
    tags=['integration', 'payments']
) }}

with payments as (
    select * from {{ ref('stg_payments') }}
),

trips as (
    select trip_id from {{ ref('int_trips_unified') }}
),

-- Check for orphan payments (no matching trip)
with_trip_check as (
    select
        p.*,
        case when t.trip_id is null then true else false end as is_orphan_payment
    from payments p
    left join trips t on p.trip_id = t.trip_id
),

-- Aggregate refunds per trip
refunds as (
    select
        trip_id,
        sum(refund_amount_local) as total_refund_local
    from payments
    where is_refund = true
    group by trip_id
),

-- Calculate net amounts
enriched as (
    select
        p.payment_id,
        p.trip_id,
        p.rider_id,
        p.driver_id,
        p.payment_type,
        p.payment_status,
        p.payment_method,
        p.card_display,
        
        -- Gross amounts
        p.amount_charged_local,
        p.currency_code,
        p.amount_to_driver_local,
        p.uber_fee_local,
        p.processing_fee_local,
        p.refund_amount_local,
        
        -- Net amounts (after refunds)
        p.amount_charged_local - coalesce(r.total_refund_local, 0) as net_amount_charged_local,
        
        -- Timestamps
        p.payment_timestamp,
        p.settlement_date,
        
        -- Flags
        p.is_refund,
        p.is_completed,
        p.is_orphan_payment,
        
        -- Reconciliation status
        case
            when p.is_orphan_payment then 'ORPHAN'
            when p.is_completed then 'RECONCILED'
            else 'PENDING'
        end as reconciliation_status,
        
        -- Metadata
        p.source_system,
        p.extracted_at,
        
        -- Audit
        current_timestamp as _int_loaded_at,
        '{{ invocation_id }}' as _int_invocation_id
        
    from with_trip_check p
    left join refunds r on p.trip_id = r.trip_id and p.is_refund = false
)

select * from enriched

{% if is_incremental() %}
    where extracted_at > (select max(extracted_at) from {{ this }})
{% endif %}

