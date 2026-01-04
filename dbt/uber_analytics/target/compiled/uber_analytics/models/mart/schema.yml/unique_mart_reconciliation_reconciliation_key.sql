
    
    

select
    reconciliation_key as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_mart"."mart_reconciliation"
where reconciliation_key is not null
group by reconciliation_key
having count(*) > 1


