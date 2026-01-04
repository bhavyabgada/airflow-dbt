
    
    

select
    snapshot_id as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_staging"."stg_surge_snapshots"
where snapshot_id is not null
group by snapshot_id
having count(*) > 1


