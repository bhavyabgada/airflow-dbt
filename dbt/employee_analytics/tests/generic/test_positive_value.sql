/*
    Generic Test: positive_value
    Description: Validates that a numeric column contains only positive values (> 0)
    Usage: Add 'positive_value' to column tests in schema.yml
*/

{% test positive_value(model, column_name) %}

with validation as (
    select
        {{ column_name }} as field_value
    from {{ model }}
    where {{ column_name }} is not null
)

select *
from validation
where field_value <= 0

{% endtest %}

