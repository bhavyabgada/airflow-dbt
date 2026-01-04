/*
================================================================================
Model: int_payments_reconciled
Layer: Integration
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#2 ORPHAN RECORDS
    Problem: Payments exist without corresponding trips (or vice versa):
             
             Causes:
             - Payment processed before trip synced (timing)
             - Trip cancelled but payment already initiated
             - Data pipeline failed mid-way
             - Manual payment adjustments
             - Refunds for trips not in our system
    
    Example:
    | payment_id | trip_id | amount | Status        |
    |------------|---------|--------|---------------|
    | PAY-001    | T-001   | $25    | MATCHED       | <- Normal
    | PAY-002    | T-999   | $50    | ORPHAN        | <- Trip T-999 doesn't exist!
    | PAY-003    | NULL    | $15    | NO_TRIP_ID    | <- No trip reference at all
    
    Business Impact: 
    - Cannot reconcile revenue
    - Possible fraud (payments without service)
    - Audit failures
    - Driver payout errors
    
    Solution:
    - LEFT JOIN payments to trips
    - Flag orphans where trip lookup returns NULL
    - Track orphan count for quality monitoring
    - Alert if orphan rate > threshold (0.1%)

--------------------------------------------------------------------------------

RECONCILIATION STATUS:
    
    - MATCHED: Payment has corresponding trip, amounts align
    - ORPHAN_PAYMENT: Payment exists but trip doesn't
    - AMOUNT_MISMATCH: Both exist but amounts differ significantly
    - REFUND: Payment is a refund (negative amount)
    - PENDING: Payment not yet completed

--------------------------------------------------------------------------------

NET AMOUNT CALCULATION:
    
    Original payment: +$100
    Service fee (Uber): -$25
    Refund (partial): -$10
    Net to driver: $65
    
    ```sql
    gross_amount - uber_fee - refund_amount as net_amount
    ```

================================================================================
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

-- Get trip data for validation
trips as (
    select 
        trip_id,
        total_fare_usd,
        trip_status,
        city_id
    from {{ ref('int_trips_unified') }}
),

-- PROBLEM #2: Identify orphan payments by left joining to trips
reconciled as (
    select
        p.payment_id,
        p.trip_id,
        p.rider_id,
        
        -- Trip info (NULL if orphan)
        t.trip_status,
        t.city_id,
        
        -- Payment details
        p.payment_method,
        p.payment_status,
        p.payment_timestamp,
        
        -- Amounts
        p.gross_amount_local,
        p.currency_code,
        p.processing_fee_local,
        p.net_amount_local,
        
        -- USD conversions (using payment date for rate)
        {{ convert_to_usd('p.gross_amount_local', 'p.currency_code', 'p.payment_timestamp') }} as gross_amount_usd,
        {{ convert_to_usd('p.net_amount_local', 'p.currency_code', 'p.payment_timestamp') }} as net_amount_usd,
        {{ convert_to_usd('p.processing_fee_local', 'p.currency_code', 'p.payment_timestamp') }} as processing_fee_usd,
        
        -- Refund tracking
        p.is_refund,
        p.refund_reason,
        p.original_payment_id,
        
        -- Payment completion status
        p.payment_status = 'COMPLETED' as is_completed,
        p.payment_status = 'PENDING' as is_pending,
        p.payment_status = 'FAILED' as is_failed,
        
        -- PROBLEM #2: Orphan detection
        -- Payment is orphan if trip_id provided but no matching trip found
        case 
            when p.trip_id is null then false  -- No trip reference (could be manual)
            when t.trip_id is null then true   -- Trip ID given but not found = ORPHAN
            else false
        end as is_orphan_payment,
        
        -- Amount validation (compare payment to trip fare)
        case
            when t.trip_id is null then 'NO_TRIP_FOUND'
            when p.is_refund then 'REFUND'
            when abs({{ convert_to_usd('p.gross_amount_local', 'p.currency_code', 'p.payment_timestamp') }} - coalesce(t.total_fare_usd, 0)) < 1.0 then 'MATCHED'
            when abs({{ convert_to_usd('p.gross_amount_local', 'p.currency_code', 'p.payment_timestamp') }} - coalesce(t.total_fare_usd, 0)) >= 1.0 then 'AMOUNT_MISMATCH'
            else 'UNKNOWN'
        end as reconciliation_status,
        
        -- Amount difference for investigation
        {{ convert_to_usd('p.gross_amount_local', 'p.currency_code', 'p.payment_timestamp') }} - coalesce(t.total_fare_usd, 0) as amount_difference_usd,
        
        -- Metadata
        p.source_system,
        p.extracted_at,
        
        -- Audit
        current_timestamp as _int_loaded_at,
        '{{ invocation_id }}' as _int_invocation_id
        
    from payments p
    left join trips t on p.trip_id = t.trip_id
)

select * from reconciled

{% if is_incremental() %}
    where extracted_at > (select max(extracted_at) from {{ this }})
{% endif %}
