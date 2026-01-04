/*
    Snapshot: employee_scd
    Type: SCD Type 2 (Slowly Changing Dimension)
    Description: 
        Tracks historical changes to employee records over time.
        Creates a new row whenever an employee's data changes,
        maintaining full history with valid_from and valid_to dates.
    
    Strategy: timestamp
    - Uses record_date to detect changes
    - Automatically manages dbt_valid_from and dbt_valid_to columns
    
    Use Cases:
    - "What was this employee's name on date X?"
    - "When did this employee's information change?"
    - Historical reporting and compliance audits
*/

{% snapshot employee_scd %}

{{
    config(
        target_database='dw',
        target_schema='dev',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='record_date',
        invalidate_hard_deletes=True
    )
}}

select
    employee_id,
    first_name,
    last_name,
    date_of_birth,
    hire_date,
    record_date,
    -- Add computed columns for tracking
    first_name || ' ' || last_name as full_name,
    current_timestamp as snapshot_loaded_at
from {{ ref('stg_employees_raw') }}

{% endsnapshot %}

