/*
    Model: dbt_audit_log
    Layer: Audit/Observability
    Description: 
        Central audit log table for tracking all dbt model executions.
        Captures start/end times, row counts, and execution status.
        Essential for monitoring, debugging, and compliance.
    
    Note: This model creates the table structure. Actual logging is done
          via pre_hook and post_hook macros in other models.
    
    Materialization: Table (created once, populated via hooks)
*/

{{ config(
    materialized='incremental',
    unique_key='log_id',
    alias='dbt_audit_log',
    on_schema_change='append_new_columns'
) }}

-- Create initial structure with seed data
-- Subsequent inserts come from pre/post hooks

{% if is_incremental() %}

-- Only select new records during incremental runs
select
    md5(event_type || model_name || invocation_id || event_timestamp::varchar) as log_id,
    event_type,
    model_name,
    schema_name,
    invocation_id,
    event_timestamp,
    status,
    row_count,
    execution_time_ms,
    run_started_at,
    current_timestamp as created_at
from {{ this }}
where 1=0  -- No new records from self-reference

{% else %}

-- Initial creation with empty structure
select
    md5('INIT' || 'init' || '{{ invocation_id }}' || current_timestamp::varchar) as log_id,
    'INIT'::varchar as event_type,
    'dbt_audit_log'::varchar as model_name,
    '{{ this.schema }}'::varchar as schema_name,
    '{{ invocation_id }}'::varchar as invocation_id,
    current_timestamp as event_timestamp,
    'SUCCESS'::varchar as status,
    0::integer as row_count,
    0::integer as execution_time_ms,
    '{{ run_started_at }}'::timestamp as run_started_at,
    current_timestamp as created_at

{% endif %}

