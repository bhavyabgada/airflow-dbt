
      -- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into "uber_analytics"."dev_integration"."int_trips_unified" as DBT_INTERNAL_DEST
        using "int_trips_unified__dbt_tmp142402098315" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.trip_id = DBT_INTERNAL_DEST.trip_id))

    
    when matched then update set
        "trip_id" = DBT_INTERNAL_SOURCE."trip_id","driver_id" = DBT_INTERNAL_SOURCE."driver_id","rider_id" = DBT_INTERNAL_SOURCE."rider_id","vehicle_id" = DBT_INTERNAL_SOURCE."vehicle_id","city_id" = DBT_INTERNAL_SOURCE."city_id","service_type_id" = DBT_INTERNAL_SOURCE."service_type_id","trip_status" = DBT_INTERNAL_SOURCE."trip_status","request_timestamp_local" = DBT_INTERNAL_SOURCE."request_timestamp_local","request_timestamp_utc" = DBT_INTERNAL_SOURCE."request_timestamp_utc","accept_timestamp_utc" = DBT_INTERNAL_SOURCE."accept_timestamp_utc","pickup_timestamp_utc" = DBT_INTERNAL_SOURCE."pickup_timestamp_utc","dropoff_timestamp_utc" = DBT_INTERNAL_SOURCE."dropoff_timestamp_utc","pickup_latitude" = DBT_INTERNAL_SOURCE."pickup_latitude","pickup_longitude" = DBT_INTERNAL_SOURCE."pickup_longitude","dropoff_latitude" = DBT_INTERNAL_SOURCE."dropoff_latitude","dropoff_longitude" = DBT_INTERNAL_SOURCE."dropoff_longitude","distance_miles" = DBT_INTERNAL_SOURCE."distance_miles","duration_minutes" = DBT_INTERNAL_SOURCE."duration_minutes","base_fare_local" = DBT_INTERNAL_SOURCE."base_fare_local","surge_multiplier" = DBT_INTERNAL_SOURCE."surge_multiplier","surge_amount_local" = DBT_INTERNAL_SOURCE."surge_amount_local","tips_local" = DBT_INTERNAL_SOURCE."tips_local","tolls_local" = DBT_INTERNAL_SOURCE."tolls_local","total_fare_local" = DBT_INTERNAL_SOURCE."total_fare_local","driver_earnings_local" = DBT_INTERNAL_SOURCE."driver_earnings_local","currency_code" = DBT_INTERNAL_SOURCE."currency_code","promo_code" = DBT_INTERNAL_SOURCE."promo_code","promo_discount_local" = DBT_INTERNAL_SOURCE."promo_discount_local","driver_rating_received" = DBT_INTERNAL_SOURCE."driver_rating_received","rider_rating_given" = DBT_INTERNAL_SOURCE."rider_rating_given","feedback_text" = DBT_INTERNAL_SOURCE."feedback_text","payment_method" = DBT_INTERNAL_SOURCE."payment_method","is_late_arrival" = DBT_INTERNAL_SOURCE."is_late_arrival","driver_source" = DBT_INTERNAL_SOURCE."driver_source","rider_source" = DBT_INTERNAL_SOURCE."rider_source","extracted_at" = DBT_INTERNAL_SOURCE."extracted_at","_int_loaded_at" = DBT_INTERNAL_SOURCE."_int_loaded_at","_int_invocation_id" = DBT_INTERNAL_SOURCE."_int_invocation_id","base_fare_usd" = DBT_INTERNAL_SOURCE."base_fare_usd","surge_amount_usd" = DBT_INTERNAL_SOURCE."surge_amount_usd","tips_usd" = DBT_INTERNAL_SOURCE."tips_usd","tolls_usd" = DBT_INTERNAL_SOURCE."tolls_usd","total_fare_usd" = DBT_INTERNAL_SOURCE."total_fare_usd","driver_earnings_usd" = DBT_INTERNAL_SOURCE."driver_earnings_usd","promo_discount_usd" = DBT_INTERNAL_SOURCE."promo_discount_usd"
    

    when not matched then insert
        ("trip_id", "driver_id", "rider_id", "vehicle_id", "city_id", "service_type_id", "trip_status", "request_timestamp_local", "request_timestamp_utc", "accept_timestamp_utc", "pickup_timestamp_utc", "dropoff_timestamp_utc", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude", "distance_miles", "duration_minutes", "base_fare_local", "surge_multiplier", "surge_amount_local", "tips_local", "tolls_local", "total_fare_local", "driver_earnings_local", "currency_code", "promo_code", "promo_discount_local", "driver_rating_received", "rider_rating_given", "feedback_text", "payment_method", "is_late_arrival", "driver_source", "rider_source", "extracted_at", "_int_loaded_at", "_int_invocation_id", "base_fare_usd", "surge_amount_usd", "tips_usd", "tolls_usd", "total_fare_usd", "driver_earnings_usd", "promo_discount_usd")
    values
        ("trip_id", "driver_id", "rider_id", "vehicle_id", "city_id", "service_type_id", "trip_status", "request_timestamp_local", "request_timestamp_utc", "accept_timestamp_utc", "pickup_timestamp_utc", "dropoff_timestamp_utc", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude", "distance_miles", "duration_minutes", "base_fare_local", "surge_multiplier", "surge_amount_local", "tips_local", "tolls_local", "total_fare_local", "driver_earnings_local", "currency_code", "promo_code", "promo_discount_local", "driver_rating_received", "rider_rating_given", "feedback_text", "payment_method", "is_late_arrival", "driver_source", "rider_source", "extracted_at", "_int_loaded_at", "_int_invocation_id", "base_fare_usd", "surge_amount_usd", "tips_usd", "tolls_usd", "total_fare_usd", "driver_earnings_usd", "promo_discount_usd")


  