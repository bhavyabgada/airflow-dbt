/*
    Generic Test: row_count_minimum
    Description: Validates that a model has at least a minimum number of rows
    Usage: 
      tests:
        - row_count_minimum:
            min_count: 100
*/

{% test row_count_minimum(model, min_count) %}

with row_count as (
    select count(*) as cnt
    from {{ model }}
)

select cnt
from row_count
where cnt < {{ min_count }}

{% endtest %}

