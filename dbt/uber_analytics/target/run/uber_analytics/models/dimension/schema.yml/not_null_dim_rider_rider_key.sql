
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "uber_analytics"."dev_quality"."not_null_dim_rider_rider_key"
    
      
    ) dbt_internal_test