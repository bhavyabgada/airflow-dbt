
    
    

select
    service_type_id as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_staging"."ref_service_types"
where service_type_id is not null
group by service_type_id
having count(*) > 1


