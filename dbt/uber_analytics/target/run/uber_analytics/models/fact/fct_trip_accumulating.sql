
      -- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into "uber_analytics"."dev_fact"."fct_trip_accumulating" as DBT_INTERNAL_DEST
        using "fct_trip_accumulating__dbt_tmp142402209011" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.trip_id = DBT_INTERNAL_DEST.trip_id))

    
    when matched then update set
        "trip_id" = DBT_INTERNAL_SOURCE."trip_id","driver_id" = DBT_INTERNAL_SOURCE."driver_id","rider_id" = DBT_INTERNAL_SOURCE."rider_id","city_id" = DBT_INTERNAL_SOURCE."city_id","service_type_id" = DBT_INTERNAL_SOURCE."service_type_id","milestone_1_requested_at" = DBT_INTERNAL_SOURCE."milestone_1_requested_at","milestone_2_assigned_at" = DBT_INTERNAL_SOURCE."milestone_2_assigned_at","milestone_3_started_at" = DBT_INTERNAL_SOURCE."milestone_3_started_at","milestone_4_completed_at" = DBT_INTERNAL_SOURCE."milestone_4_completed_at","milestone_5_paid_at" = DBT_INTERNAL_SOURCE."milestone_5_paid_at","trip_status" = DBT_INTERNAL_SOURCE."trip_status","current_milestone" = DBT_INTERNAL_SOURCE."current_milestone","current_milestone_name" = DBT_INTERNAL_SOURCE."current_milestone_name","minutes_request_to_assign" = DBT_INTERNAL_SOURCE."minutes_request_to_assign","minutes_assign_to_pickup" = DBT_INTERNAL_SOURCE."minutes_assign_to_pickup","minutes_pickup_to_dropoff" = DBT_INTERNAL_SOURCE."minutes_pickup_to_dropoff","minutes_dropoff_to_payment" = DBT_INTERNAL_SOURCE."minutes_dropoff_to_payment","total_lifecycle_minutes" = DBT_INTERNAL_SOURCE."total_lifecycle_minutes","total_fare_usd" = DBT_INTERNAL_SOURCE."total_fare_usd","driver_earnings_usd" = DBT_INTERNAL_SOURCE."driver_earnings_usd","payment_count" = DBT_INTERNAL_SOURCE."payment_count","refund_count" = DBT_INTERNAL_SOURCE."refund_count","is_lifecycle_complete" = DBT_INTERNAL_SOURCE."is_lifecycle_complete","days_since_request" = DBT_INTERNAL_SOURCE."days_since_request","is_late_arrival" = DBT_INTERNAL_SOURCE."is_late_arrival","last_updated_at" = DBT_INTERNAL_SOURCE."last_updated_at","_fact_loaded_at" = DBT_INTERNAL_SOURCE."_fact_loaded_at","_invocation_id" = DBT_INTERNAL_SOURCE."_invocation_id"
    

    when not matched then insert
        ("trip_id", "driver_id", "rider_id", "city_id", "service_type_id", "milestone_1_requested_at", "milestone_2_assigned_at", "milestone_3_started_at", "milestone_4_completed_at", "milestone_5_paid_at", "trip_status", "current_milestone", "current_milestone_name", "minutes_request_to_assign", "minutes_assign_to_pickup", "minutes_pickup_to_dropoff", "minutes_dropoff_to_payment", "total_lifecycle_minutes", "total_fare_usd", "driver_earnings_usd", "payment_count", "refund_count", "is_lifecycle_complete", "days_since_request", "is_late_arrival", "last_updated_at", "_fact_loaded_at", "_invocation_id")
    values
        ("trip_id", "driver_id", "rider_id", "city_id", "service_type_id", "milestone_1_requested_at", "milestone_2_assigned_at", "milestone_3_started_at", "milestone_4_completed_at", "milestone_5_paid_at", "trip_status", "current_milestone", "current_milestone_name", "minutes_request_to_assign", "minutes_assign_to_pickup", "minutes_pickup_to_dropoff", "minutes_dropoff_to_payment", "total_lifecycle_minutes", "total_fare_usd", "driver_earnings_usd", "payment_count", "refund_count", "is_lifecycle_complete", "days_since_request", "is_late_arrival", "last_updated_at", "_fact_loaded_at", "_invocation_id")


  