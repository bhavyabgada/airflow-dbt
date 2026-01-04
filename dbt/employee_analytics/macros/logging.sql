/*
    Macro: log_model_start
    Description: Logs the start of a model run to the audit table.
                 Use as a pre-hook in model config.
    
    Usage in model:
        {{ config(
            pre_hook="{{ log_model_start() }}"
        ) }}
*/

{% macro log_model_start() %}
    {% if execute and target.name != 'ci' %}
        {% set log_query %}
            insert into {{ target.schema }}.dbt_audit_log 
            (event_type, model_name, schema_name, invocation_id, event_timestamp, status, row_count, execution_time_ms, run_started_at)
            values (
                'MODEL_START',
                '{{ this.identifier }}',
                '{{ this.schema }}',
                '{{ invocation_id }}',
                current_timestamp,
                'RUNNING',
                null,
                null,
                '{{ run_started_at }}'
            )
        {% endset %}
        {% do run_query(log_query) %}
    {% endif %}
{% endmacro %}


/*
    Macro: log_model_end
    Description: Logs the completion of a model run.
                 Use as a post-hook in model config.
    
    Usage in model:
        {{ config(
            post_hook="{{ log_model_end() }}"
        ) }}
*/

{% macro log_model_end() %}
    {% if execute and target.name != 'ci' %}
        {% set log_query %}
            insert into {{ target.schema }}.dbt_audit_log 
            (event_type, model_name, schema_name, invocation_id, event_timestamp, status, row_count, execution_time_ms, run_started_at)
            values (
                'MODEL_END',
                '{{ this.identifier }}',
                '{{ this.schema }}',
                '{{ invocation_id }}',
                current_timestamp,
                'SUCCESS',
                (select count(*) from {{ this }}),
                null,
                '{{ run_started_at }}'
            )
        {% endset %}
        {% do run_query(log_query) %}
    {% endif %}
{% endmacro %}


/*
    Macro: get_row_count
    Description: Returns the row count of a model/table.
                 Useful for data quality checks and monitoring.
    
    Usage:
        {% set count = get_row_count(ref('my_model')) %}
*/

{% macro get_row_count(relation) %}
    {% set query %}
        select count(*) as cnt from {{ relation }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {{ return(results.columns[0].values()[0]) }}
    {% endif %}
{% endmacro %}


/*
    Macro: assert_row_count
    Description: Fails if row count is below minimum threshold.
                 Use for critical data quality checks.
*/

{% macro assert_row_count(model, min_count) %}
    {% set query %}
        select count(*) as cnt from {{ model }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {% set actual_count = results.columns[0].values()[0] %}
        {% if actual_count < min_count %}
            {{ exceptions.raise_compiler_error("Model " ~ model ~ " has " ~ actual_count ~ " rows, expected at least " ~ min_count) }}
        {% endif %}
    {% endif %}
{% endmacro %}

