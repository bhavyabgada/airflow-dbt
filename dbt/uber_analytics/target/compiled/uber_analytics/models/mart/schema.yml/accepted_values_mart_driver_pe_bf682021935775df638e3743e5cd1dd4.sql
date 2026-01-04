
    
    

with all_values as (

    select
        engagement_status as value_field,
        count(*) as n_records

    from "uber_analytics"."dev_mart"."mart_driver_performance"
    group by engagement_status

)

select *
from all_values
where value_field not in (
    'ACTIVE','RECENT','AT_RISK','CHURNED'
)


