/*
    Generic Test: date_in_past
    Description: Validates that a date column contains only past dates (not future)
    Usage: Add 'date_in_past' to column tests in schema.yml
*/

{% test date_in_past(model, column_name) %}

with validation as (
    select
        {{ column_name }} as date_value
    from {{ model }}
    where {{ column_name }} is not null
)

select *
from validation
where date_value > current_date

{% endtest %}

