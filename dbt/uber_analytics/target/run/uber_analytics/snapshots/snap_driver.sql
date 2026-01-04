
      
  
    

  create  table "uber_analytics"."dimension"."snap_driver"
  
  
    as
  
  (
    
    

    select *,
        md5(coalesce(cast(driver_id as varchar ), '')
         || '|' || coalesce(cast(now()::timestamp without time zone as varchar ), '')
        ) as dbt_scd_id,
        now()::timestamp without time zone as dbt_updated_at,
        now()::timestamp without time zone as dbt_valid_from,
        
  
  coalesce(nullif(now()::timestamp without time zone, now()::timestamp without time zone), null)
  as dbt_valid_to
from (
        



select
    driver_id,
    
    -- PII (hashed in staging)
    
    first_name_hash,
    last_name_hash,
    email_hash,
    phone_hash,
    ssn_masked,
    
    
    date_of_birth,
    license_number,
    license_state,
    license_expiry,
    city_id,
    driver_status,
    current_rating,
    total_trips,
    signup_date,
    last_active_date,
    background_check_status,
    background_check_date,
    is_active,
    _row_hash,
    extracted_at as updated_at
    
from "uber_analytics"."dev_staging"."stg_drivers"

    ) sbq



  );
  
  