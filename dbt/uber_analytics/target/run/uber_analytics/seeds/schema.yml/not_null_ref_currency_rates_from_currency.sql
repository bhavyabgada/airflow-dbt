
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "uber_analytics"."dev_quality"."not_null_ref_currency_rates_from_currency"
    
      
    ) dbt_internal_test