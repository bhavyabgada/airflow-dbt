
    
    

with all_values as (

    select
        reconciliation_status as value_field,
        count(*) as n_records

    from "uber_analytics"."dev_integration"."int_payments_reconciled"
    group by reconciliation_status

)

select *
from all_values
where value_field not in (
    'ORPHAN','RECONCILED','PENDING'
)


