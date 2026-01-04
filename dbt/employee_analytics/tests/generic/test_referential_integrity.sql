/*
    Generic Test: referential_integrity
    Description: Validates that all values in a column exist in a reference table
    Usage:
      tests:
        - referential_integrity:
            to_model: ref('dim_departments')
            to_column: department_id
*/

{% test referential_integrity(model, column_name, to_model, to_column) %}

with source_values as (
    select distinct {{ column_name }} as value
    from {{ model }}
    where {{ column_name }} is not null
),

reference_values as (
    select distinct {{ to_column }} as value
    from {{ to_model }}
)

select source_values.value
from source_values
left join reference_values
    on source_values.value = reference_values.value
where reference_values.value is null

{% endtest %}

