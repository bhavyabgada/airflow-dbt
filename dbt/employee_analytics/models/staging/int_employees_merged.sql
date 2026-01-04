/*
    Model: int_employees_merged
    Layer: Intermediate/Staging
    Description: Employee dimension table using MERGE incremental strategy.
                 Deduplicates records and maintains the latest version of each employee.
    Materialization: Incremental (merge on employee_id)
*/

{{ config(
    materialized='incremental',
    alias='employees_current',
    unique_key='employee_id'
) }}

with transformed as (

    select
        cast(employee_id as integer) as employee_id,
        first_name,
        last_name,
        to_date(date_of_birth, 'DD-MM-YYYY') as date_of_birth,
        hire_date,
        record_date

    from {{ ref('stg_employees_raw') }}

),

deduplicated as (

    select 
        *,
        row_number() over (
            partition by employee_id 
            order by record_date desc
        ) as row_num
    from transformed

)

select 
    employee_id,
    first_name,
    last_name,
    date_of_birth,
    hire_date,
    record_date
from deduplicated
where row_num = 1
