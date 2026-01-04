
    
    

select
    full_date as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_dimension"."dim_date"
where full_date is not null
group by full_date
having count(*) > 1


