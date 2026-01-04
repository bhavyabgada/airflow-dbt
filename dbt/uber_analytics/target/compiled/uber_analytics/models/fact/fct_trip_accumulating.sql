/*
    Model: fct_trip_accumulating
    Layer: Fact
    Description: 
        Accumulating snapshot fact table tracking trip lifecycle.
        Records timestamps at each milestone: requested → assigned → started → completed → paid.
    
    Problems Addressed:
    - #26 Accumulating Snapshot Facts: Track lifecycle stages
*/



with trips as (
    select * from "uber_analytics"."dev_integration"."int_trips_unified"
),

payments as (
    select 
        trip_id,
        min(payment_timestamp) as first_payment_timestamp,
        max(payment_timestamp) as last_payment_timestamp,
        count(*) as payment_count,
        sum(case when is_refund then 1 else 0 end) as refund_count
    from "uber_analytics"."dev_integration"."int_payments_reconciled"
    where is_completed = true
    group by trip_id
),

accumulating as (
    select
        -- Keys
        t.trip_id,
        t.driver_id,
        t.rider_id,
        t.city_id,
        t.service_type_id,
        
        -- Lifecycle timestamps
        t.request_timestamp_utc as milestone_1_requested_at,
        t.accept_timestamp_utc as milestone_2_assigned_at,
        t.pickup_timestamp_utc as milestone_3_started_at,
        t.dropoff_timestamp_utc as milestone_4_completed_at,
        p.first_payment_timestamp as milestone_5_paid_at,
        
        -- Current status
        t.trip_status,
        
        -- Which milestone was reached?
        case
            when p.first_payment_timestamp is not null then 5
            when t.dropoff_timestamp_utc is not null then 4
            when t.pickup_timestamp_utc is not null then 3
            when t.accept_timestamp_utc is not null then 2
            when t.request_timestamp_utc is not null then 1
            else 0
        end as current_milestone,
        
        case
            when p.first_payment_timestamp is not null then 'PAID'
            when t.dropoff_timestamp_utc is not null then 'COMPLETED'
            when t.pickup_timestamp_utc is not null then 'IN_PROGRESS'
            when t.accept_timestamp_utc is not null then 'ASSIGNED'
            when t.request_timestamp_utc is not null then 'REQUESTED'
            else 'UNKNOWN'
        end as current_milestone_name,
        
        -- Duration between milestones (minutes)
        extract(epoch from (t.accept_timestamp_utc - t.request_timestamp_utc))/60 
            as minutes_request_to_assign,
        extract(epoch from (t.pickup_timestamp_utc - t.accept_timestamp_utc))/60 
            as minutes_assign_to_pickup,
        extract(epoch from (t.dropoff_timestamp_utc - t.pickup_timestamp_utc))/60 
            as minutes_pickup_to_dropoff,
        extract(epoch from (p.first_payment_timestamp - t.dropoff_timestamp_utc))/60 
            as minutes_dropoff_to_payment,
        
        -- Total lifecycle duration
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
        
        -- Is lifecycle complete?
        case when p.first_payment_timestamp is not null then true else false end as is_lifecycle_complete,
        
        -- Days since request (for aging)
        current_date - t.request_timestamp_utc::date as days_since_request,
        
        -- Late arrival flag
        t.is_late_arrival,
        
        -- Audit
        greatest(t.extracted_at, coalesce(p.last_payment_timestamp, t.extracted_at)) as last_updated_at,
        current_timestamp as _fact_loaded_at,
        '402658b7-059e-4de5-83c3-55909b7e2657' as _invocation_id
        
    from trips t
    left join payments p on t.trip_id = p.trip_id
)

select * from accumulating


    where last_updated_at > (select max(last_updated_at) from "uber_analytics"."dev_fact"."fct_trip_accumulating")
       or is_lifecycle_complete = false  -- Always update incomplete trips
