
    
    

select
    revenue_key as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_mart"."mart_revenue_daily"
where revenue_key is not null
group by revenue_key
having count(*) > 1


