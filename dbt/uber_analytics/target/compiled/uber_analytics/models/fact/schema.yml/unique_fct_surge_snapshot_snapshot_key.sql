
    
    

select
    snapshot_key as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_fact"."fct_surge_snapshot"
where snapshot_key is not null
group by snapshot_key
having count(*) > 1


