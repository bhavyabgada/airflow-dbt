/*
================================================================================
Model: fct_trip_accumulating
Layer: Fact
Type: Accumulating Snapshot Fact
Grain: One row per trip (updated as trip progresses)
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#20 ACCUMULATING SNAPSHOT FACTS
    Problem: A trip goes through multiple lifecycle stages over time:
             1. REQUESTED: Customer opens app, requests ride
             2. ASSIGNED: Driver accepts the request
             3. STARTED: Driver picks up customer
             4. COMPLETED: Trip ends at destination
             5. PAID: Payment successfully processed
    
             Traditional transaction facts only capture the FINAL state.
             But operations teams need to track:
             - How long from request to driver assignment? (acceptance rate)
             - How long from assignment to pickup? (driver efficiency)
             - How many trips are stuck at each stage?
             - What's the average total lifecycle time?
    
    Business Impact: Cannot measure operational KPIs, hard to identify bottlenecks.
    
    Solution: ACCUMULATING SNAPSHOT FACT TABLE
    - One row per trip (like transaction fact)
    - BUT: Multiple milestone timestamp columns
    - Row is UPDATED as trip progresses through stages
    - Unlike periodic snapshots which INSERT new rows, this UPDATES existing row
    
    Schema Design:
    ```sql
    trip_id                    -- Grain
    milestone_1_requested_at   -- Stage timestamps
    milestone_2_assigned_at
    milestone_3_started_at
    milestone_4_completed_at
    milestone_5_paid_at
    current_milestone          -- Where is trip now? (1-5)
    is_lifecycle_complete      -- Has it reached final stage?
    ```

--------------------------------------------------------------------------------

LAG ANALYSIS:
    Time between milestones reveals operational health:
    
    - minutes_request_to_assign: Driver supply metric
      - Long lag = not enough drivers in area
      
    - minutes_assign_to_pickup: Driver efficiency
      - Long lag = traffic, wrong route, slow driver
      
    - minutes_pickup_to_dropoff: Trip duration
      - Compare with estimated time
      
    - minutes_dropoff_to_payment: Payment processing
      - Should be <1 minute for card payments

--------------------------------------------------------------------------------

INCREMENTAL LOGIC:
    Unlike regular incrementals, we need to UPDATE incomplete trips:
    
    ```sql
    where last_updated_at > (select max(last_updated_at) from {{ this }})
       or is_lifecycle_complete = false  -- Always re-check incomplete trips
    ```
    
    This ensures trips stuck in middle stages eventually get updated
    when their next milestone is recorded.

================================================================================
*/

{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    tags=['fact', 'accumulating']
) }}

with trips as (
    select * from {{ ref('int_trips_unified') }}
),

-- Aggregate payment info per trip
payments as (
    select 
        trip_id,
        min(payment_timestamp) as first_payment_timestamp,
        max(payment_timestamp) as last_payment_timestamp,
        count(*) as payment_count,
        sum(case when is_refund then 1 else 0 end) as refund_count
    from {{ ref('int_payments_reconciled') }}
    where is_completed = true
    group by trip_id
),

-- Build accumulating snapshot
accumulating as (
    select
        -- Grain: One row per trip
        t.trip_id,
        t.driver_id,
        t.rider_id,
        t.city_id,
        t.service_type_id,
        
        -- MILESTONE TIMESTAMPS (the core of accumulating snapshot)
        t.request_timestamp_utc as milestone_1_requested_at,
        t.accept_timestamp_utc as milestone_2_assigned_at,
        t.pickup_timestamp_utc as milestone_3_started_at,
        t.dropoff_timestamp_utc as milestone_4_completed_at,
        p.first_payment_timestamp as milestone_5_paid_at,
        
        -- Current trip status
        t.trip_status,
        
        -- CURRENT MILESTONE: Which stage has the trip reached?
        case
            when p.first_payment_timestamp is not null then 5
            when t.dropoff_timestamp_utc is not null then 4
            when t.pickup_timestamp_utc is not null then 3
            when t.accept_timestamp_utc is not null then 2
            when t.request_timestamp_utc is not null then 1
            else 0
        end as current_milestone,
        
        -- Human-readable milestone name
        case
            when p.first_payment_timestamp is not null then 'PAID'
            when t.dropoff_timestamp_utc is not null then 'COMPLETED'
            when t.pickup_timestamp_utc is not null then 'IN_PROGRESS'
            when t.accept_timestamp_utc is not null then 'ASSIGNED'
            when t.request_timestamp_utc is not null then 'REQUESTED'
            else 'UNKNOWN'
        end as current_milestone_name,
        
        -- LAG ANALYSIS: Time between milestones (minutes)
        extract(epoch from (t.accept_timestamp_utc - t.request_timestamp_utc))/60 
            as minutes_request_to_assign,
        extract(epoch from (t.pickup_timestamp_utc - t.accept_timestamp_utc))/60 
            as minutes_assign_to_pickup,
        extract(epoch from (t.dropoff_timestamp_utc - t.pickup_timestamp_utc))/60 
            as minutes_pickup_to_dropoff,
        extract(epoch from (p.first_payment_timestamp - t.dropoff_timestamp_utc))/60 
            as minutes_dropoff_to_payment,
        
        -- Total lifecycle duration (request to current milestone)
        extract(epoch from (
            coalesce(p.first_payment_timestamp, t.dropoff_timestamp_utc, 
                     t.pickup_timestamp_utc, t.accept_timestamp_utc, t.request_timestamp_utc)
            - t.request_timestamp_utc
        ))/60 as total_lifecycle_minutes,
        
        -- Financial measures
        t.total_fare_usd,
        t.driver_earnings_usd,
        
        -- Payment info
        p.payment_count,
        p.refund_count,
        
        -- Is trip lifecycle complete? (reached final stage)
        case when p.first_payment_timestamp is not null then true else false end as is_lifecycle_complete,
        
        -- Days since request (for aging analysis)
        current_date - t.request_timestamp_utc::date as days_since_request,
        
        -- Late arrival flag
        t.is_late_arrival,
        
        -- Last update timestamp (for incremental processing)
        greatest(t.extracted_at, coalesce(p.last_payment_timestamp, t.extracted_at)) as last_updated_at,
        
        -- Audit
        current_timestamp as _fact_loaded_at,
        '{{ invocation_id }}' as _invocation_id
        
    from trips t
    left join payments p on t.trip_id = p.trip_id
)

select * from accumulating

{% if is_incremental() %}
    -- Re-process recently updated AND incomplete trips
    where last_updated_at > (select max(last_updated_at) from {{ this }})
       or is_lifecycle_complete = false  -- Always update incomplete trips
{% endif %}
