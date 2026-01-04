
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "uber_analytics"."dev_quality"."unique_bridge_driver_vehicle_bridge_key"
    
      
    ) dbt_internal_test