
      
  
    

  create  table "uber_analytics"."dimension"."snap_rider"
  
  
    as
  
  (
    
    

    select *,
        md5(coalesce(cast(rider_id as varchar ), '')
         || '|' || coalesce(cast(now()::timestamp without time zone as varchar ), '')
        ) as dbt_scd_id,
        now()::timestamp without time zone as dbt_updated_at,
        now()::timestamp without time zone as dbt_valid_from,
        
  
  coalesce(nullif(now()::timestamp without time zone, now()::timestamp without time zone), null)
  as dbt_valid_to
from (
        



select
    rider_id,
    
    
    first_name_hash,
    last_name_hash,
    email_hash,
    phone_hash,
    
    
    date_of_birth,
    city_id,
    rider_tier,
    lifetime_trips,
    lifetime_spend_usd,
    signup_date,
    last_trip_date,
    preferred_payment_method,
    is_active,
    fraud_flag,
    data_retention_until,
    extracted_at as updated_at
    
from "uber_analytics"."dev_staging"."stg_riders"

    ) sbq



  );
  
  