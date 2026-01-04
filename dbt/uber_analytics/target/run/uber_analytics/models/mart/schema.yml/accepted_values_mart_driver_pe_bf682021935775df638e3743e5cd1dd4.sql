
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "uber_analytics"."dev_quality"."accepted_values_mart_driver_pe_bf682021935775df638e3743e5cd1dd4"
    
      
    ) dbt_internal_test