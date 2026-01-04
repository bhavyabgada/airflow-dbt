
    
    

select
    currency_code as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_staging"."ref_currencies"
where currency_code is not null
group by currency_code
having count(*) > 1


