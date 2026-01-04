
    
    

select
    date_key as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_dimension"."dim_date"
where date_key is not null
group by date_key
having count(*) > 1


