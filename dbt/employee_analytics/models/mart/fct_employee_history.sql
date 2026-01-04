/*
    Model: fct_employee_history
    Layer: Mart/Analytics
    Description: Fact table containing historical employee records.
                 Uses incremental APPEND strategy to maintain full history.
                 New records are added based on execution_date variable.
    Materialization: Incremental (append)
*/

{{ config(
    materialized='incremental', 
    alias='fact_employee_history'
) }}

with employee_records as (

    select
        employee_id,
        first_name,
        last_name,
        date_of_birth,
        hire_date,
        record_date,
        current_timestamp as loaded_at

    from {{ ref('int_employees_merged') }}

    {% if is_incremental() %}
        where record_date = '{{ var("execution_date") }}'
    {% endif %}

)

select * 
from employee_records
