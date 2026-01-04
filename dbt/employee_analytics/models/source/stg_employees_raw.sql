/*
    Model: stg_employees_raw
    Layer: Source/Staging
    Description: Raw employee data ingestion layer. This table contains
                 the source employee records as-is from the source system.
    Materialization: Table (full refresh)
*/

{{ config(
    materialized='table',
    alias='employees_raw'
) }}

with source_data as (

    select 
        1 as employee_id,
        'Ron' as first_name,
        'Paul' as last_name,
        '21-08-1969' as date_of_birth,
        '2022-08-01 09:00:00' as hire_date,
        '2022-09-01' as record_date
    union all
    select
        2 as employee_id,
        'Ben' as first_name,
        'Stack' as last_name,
        '01-09-1975' as date_of_birth,
        '2022-09-04 09:00:00' as hire_date,
        '2022-09-02' as record_date
    union all
    select
        4 as employee_id,
        'Jason' as first_name,
        'Tex' as last_name,
        '26-06-1979' as date_of_birth,
        '2022-10-02 09:00:00' as hire_date,
        '2022-09-02' as record_date

)

select *
from source_data
