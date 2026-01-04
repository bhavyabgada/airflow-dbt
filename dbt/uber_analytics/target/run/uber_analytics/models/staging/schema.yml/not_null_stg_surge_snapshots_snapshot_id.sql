
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "uber_analytics"."dev_quality"."not_null_stg_surge_snapshots_snapshot_id"
    
      
    ) dbt_internal_test