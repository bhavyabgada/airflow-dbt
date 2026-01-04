
      -- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into "uber_analytics"."dev_integration"."int_payments_reconciled" as DBT_INTERNAL_DEST
        using "int_payments_reconciled__dbt_tmp142402168491" as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.payment_id = DBT_INTERNAL_DEST.payment_id))

    
    when matched then update set
        "payment_id" = DBT_INTERNAL_SOURCE."payment_id","trip_id" = DBT_INTERNAL_SOURCE."trip_id","rider_id" = DBT_INTERNAL_SOURCE."rider_id","driver_id" = DBT_INTERNAL_SOURCE."driver_id","payment_type" = DBT_INTERNAL_SOURCE."payment_type","payment_status" = DBT_INTERNAL_SOURCE."payment_status","payment_method" = DBT_INTERNAL_SOURCE."payment_method","card_display" = DBT_INTERNAL_SOURCE."card_display","amount_charged_local" = DBT_INTERNAL_SOURCE."amount_charged_local","currency_code" = DBT_INTERNAL_SOURCE."currency_code","amount_to_driver_local" = DBT_INTERNAL_SOURCE."amount_to_driver_local","uber_fee_local" = DBT_INTERNAL_SOURCE."uber_fee_local","processing_fee_local" = DBT_INTERNAL_SOURCE."processing_fee_local","refund_amount_local" = DBT_INTERNAL_SOURCE."refund_amount_local","net_amount_charged_local" = DBT_INTERNAL_SOURCE."net_amount_charged_local","payment_timestamp" = DBT_INTERNAL_SOURCE."payment_timestamp","settlement_date" = DBT_INTERNAL_SOURCE."settlement_date","is_refund" = DBT_INTERNAL_SOURCE."is_refund","is_completed" = DBT_INTERNAL_SOURCE."is_completed","is_orphan_payment" = DBT_INTERNAL_SOURCE."is_orphan_payment","reconciliation_status" = DBT_INTERNAL_SOURCE."reconciliation_status","source_system" = DBT_INTERNAL_SOURCE."source_system","extracted_at" = DBT_INTERNAL_SOURCE."extracted_at","_int_loaded_at" = DBT_INTERNAL_SOURCE."_int_loaded_at","_int_invocation_id" = DBT_INTERNAL_SOURCE."_int_invocation_id"
    

    when not matched then insert
        ("payment_id", "trip_id", "rider_id", "driver_id", "payment_type", "payment_status", "payment_method", "card_display", "amount_charged_local", "currency_code", "amount_to_driver_local", "uber_fee_local", "processing_fee_local", "refund_amount_local", "net_amount_charged_local", "payment_timestamp", "settlement_date", "is_refund", "is_completed", "is_orphan_payment", "reconciliation_status", "source_system", "extracted_at", "_int_loaded_at", "_int_invocation_id")
    values
        ("payment_id", "trip_id", "rider_id", "driver_id", "payment_type", "payment_status", "payment_method", "card_display", "amount_charged_local", "currency_code", "amount_to_driver_local", "uber_fee_local", "processing_fee_local", "refund_amount_local", "net_amount_charged_local", "payment_timestamp", "settlement_date", "is_refund", "is_completed", "is_orphan_payment", "reconciliation_status", "source_system", "extracted_at", "_int_loaded_at", "_int_invocation_id")


  