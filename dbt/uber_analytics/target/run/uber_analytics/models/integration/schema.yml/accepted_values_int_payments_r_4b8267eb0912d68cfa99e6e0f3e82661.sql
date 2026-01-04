
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "uber_analytics"."dev_quality"."accepted_values_int_payments_r_4b8267eb0912d68cfa99e6e0f3e82661"
    
      
    ) dbt_internal_test