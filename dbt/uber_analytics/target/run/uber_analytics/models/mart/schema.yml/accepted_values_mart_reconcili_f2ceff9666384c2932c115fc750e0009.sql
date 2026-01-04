
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "uber_analytics"."dev_quality"."accepted_values_mart_reconcili_f2ceff9666384c2932c115fc750e0009"
    
      
    ) dbt_internal_test