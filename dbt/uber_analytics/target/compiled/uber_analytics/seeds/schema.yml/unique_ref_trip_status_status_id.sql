
    
    

select
    status_id as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_staging"."ref_trip_status"
where status_id is not null
group by status_id
having count(*) > 1


