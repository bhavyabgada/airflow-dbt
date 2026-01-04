
    
    

select
    driver_id as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_staging"."stg_drivers"
where driver_id is not null
group by driver_id
having count(*) > 1


