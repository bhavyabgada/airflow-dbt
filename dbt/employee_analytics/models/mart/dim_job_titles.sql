/*
    Model: dim_job_titles
    Layer: Mart/Dimension
    Description: Job title dimension table sourced from seed data.
                 Provides the organizational job ladder with levels and families.
    Materialization: Table
*/

{{ config(
    materialized='table',
    alias='dim_job_titles'
) }}

with job_titles as (
    select
        job_title_id,
        job_title,
        job_level,
        job_family,
        is_management,
        -- Derived columns
        case job_level
            when 1 then 'Entry Level'
            when 2 then 'Mid Level'
            when 3 then 'Senior'
            when 4 then 'Lead/Staff'
            when 5 then 'Director'
            when 6 then 'VP'
            when 7 then 'C-Level'
        end as level_description,
        -- Add audit columns
        current_timestamp as loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id
    from {{ ref('job_titles') }}
)

select * from job_titles

