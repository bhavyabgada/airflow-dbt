/*
    Generic Test: not_empty_string
    Description: Validates that a string column is not empty or whitespace-only
    Usage: Add 'not_empty_string' to column tests in schema.yml
*/

{% test not_empty_string(model, column_name) %}

with validation as (
    select
        {{ column_name }} as field_value
    from {{ model }}
    where {{ column_name }} is not null
)

select *
from validation
where trim(field_value) = ''

{% endtest %}

