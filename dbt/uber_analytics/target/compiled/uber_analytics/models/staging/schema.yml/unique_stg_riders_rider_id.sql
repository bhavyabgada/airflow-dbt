
    
    

select
    rider_id as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_staging"."stg_riders"
where rider_id is not null
group by rider_id
having count(*) > 1


