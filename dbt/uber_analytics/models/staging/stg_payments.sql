/*
    Model: stg_payments
    Source: Payments System
    Description: Staging model for payment transactions.
                 Handles refunds, settlements, and payment reconciliation prep.
    
    Problems Addressed:
    - #3 Data Reconciliation: Prepares data for trip-payment matching
    - #14 Idempotency: Uses payment_id as idempotency key
*/

{{ config(
    materialized='view',
    tags=['staging', 'payments']
) }}

with source_data as (
    select * from {{ ref('source_payments') }}
),

standardized as (
    select
        -- Payment identifiers
        payment_id,
        trip_id,
        rider_id,
        driver_id,
        
        -- Payment classification
        payment_type,
        payment_status,
        payment_method,
        
        -- Card info (masked)
        {% if var('mask_pii', true) %}
        case 
            when card_last_four is not null 
            then '****' || card_last_four 
            else null 
        end as card_display,
        {% else %}
        card_last_four,
        {% endif %}
        
        -- Financial amounts (in transaction currency)
        amount_charged::decimal(12,2) as amount_charged_local,
        currency_code,
        amount_to_driver::decimal(12,2) as amount_to_driver_local,
        uber_fee::decimal(12,2) as uber_fee_local,
        processing_fee::decimal(12,2) as processing_fee_local,
        refund_amount::decimal(12,2) as refund_amount_local,
        
        -- Timestamps
        payment_timestamp::timestamp as payment_timestamp,
        settlement_date::date as settlement_date,
        
        -- Derived flags
        case 
            when payment_type = 'REFUND' then true 
            else false 
        end as is_refund,
        
        case 
            when payment_status = 'COMPLETED' then true 
            else false 
        end as is_completed,
        
        -- Metadata
        source_system,
        extracted_at::timestamp as extracted_at,
        
        -- Audit columns
        current_timestamp as _stg_loaded_at,
        '{{ invocation_id }}' as _stg_invocation_id
        
    from source_data
)

select * from standardized

