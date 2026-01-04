/*
    Model: dim_departments
    Layer: Mart/Dimension
    Description: Department dimension table sourced from seed data.
                 Provides reference information for organizational structure.
    Materialization: Table
*/

{{ config(
    materialized='table',
    alias='dim_departments'
) }}

with departments as (
    select
        department_id,
        department_name,
        department_code,
        cost_center,
        is_active,
        -- Add audit columns
        current_timestamp as loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id
    from {{ ref('departments') }}
)

select * from departments

