
    
    

select
    earnings_key as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_fact"."fct_driver_earnings"
where earnings_key is not null
group by earnings_key
having count(*) > 1


