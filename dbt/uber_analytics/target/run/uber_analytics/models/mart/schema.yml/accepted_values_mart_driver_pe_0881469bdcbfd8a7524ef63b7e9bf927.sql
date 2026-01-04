
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "uber_analytics"."dev_quality"."accepted_values_mart_driver_pe_0881469bdcbfd8a7524ef63b7e9bf927"
    
      
    ) dbt_internal_test