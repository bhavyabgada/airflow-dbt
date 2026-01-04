
    
    

select
    payment_id as unique_field,
    count(*) as n_records

from "uber_analytics"."dev_integration"."int_payments_reconciled"
where payment_id is not null
group by payment_id
having count(*) > 1


