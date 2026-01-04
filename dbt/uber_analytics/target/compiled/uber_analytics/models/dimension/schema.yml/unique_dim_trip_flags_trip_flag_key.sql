
    
    

select
    trip_flag_key as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_dimension"."dim_trip_flags"
where trip_flag_key is not null
group by trip_flag_key
having count(*) > 1


