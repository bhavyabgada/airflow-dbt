
    
    

with all_values as (

    select
        performance_tier as value_field,
        count(*) as n_records

    from "uber_analytics"."dev_mart"."mart_driver_performance"
    group by performance_tier

)

select *
from all_values
where value_field not in (
    'PLATINUM','GOLD','SILVER','BRONZE'
)


