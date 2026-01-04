
    
    

select
    bridge_key as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_dimension"."bridge_driver_vehicle"
where bridge_key is not null
group by bridge_key
having count(*) > 1


