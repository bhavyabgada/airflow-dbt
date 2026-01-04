
    
    

select
    driver_key as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_mart"."mart_driver_performance"
where driver_key is not null
group by driver_key
having count(*) > 1


