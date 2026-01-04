
    
    

select
    trip_id as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_fact"."fct_trip_accumulating"
where trip_id is not null
group by trip_id
having count(*) > 1


