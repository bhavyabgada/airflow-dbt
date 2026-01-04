/*
    Data Quality Macros
    ===================
    Problems: #3 Reconciliation, #34 Circuit Breaker
*/

/*
    Macro: create_audit_entry
    Description: Creates an audit log entry for pipeline runs.
*/
{% macro create_audit_entry(event_type) %}
    {% if execute %}
        {% set query %}
            insert into {{ target.schema }}.pipeline_audit_log 
            (event_type, invocation_id, event_timestamp, target_name)
            values (
                '{{ event_type }}',
                '{{ invocation_id }}',
                current_timestamp,
                '{{ target.name }}'
            )
        {% endset %}
        {% do run_query(query) %}
    {% endif %}
{% endmacro %}


/*
    Macro: check_reconciliation
    Description: Compares row counts between source and target.
                 Returns the match rate (0.0 to 1.0).
    
    Usage:
        {% set match_rate = check_reconciliation(ref('source'), ref('target'), 'trip_id') %}
*/
{% macro check_reconciliation(source_model, target_model, key_column) %}
    {% set query %}
        with source_keys as (
            select distinct {{ key_column }} from {{ source_model }}
        ),
        target_keys as (
            select distinct {{ key_column }} from {{ target_model }}
        ),
        comparison as (
            select
                (select count(*) from source_keys) as source_count,
                (select count(*) from target_keys) as target_count,
                (
                    select count(*) 
                    from source_keys s 
                    inner join target_keys t on s.{{ key_column }} = t.{{ key_column }}
                ) as matched_count
        )
        select 
            matched_count::decimal / nullif(source_count, 0) as match_rate
        from comparison
    {% endset %}
    
    {% set results = run_query(query) %}
    {% if execute %}
        {{ return(results.columns[0].values()[0]) }}
    {% endif %}
{% endmacro %}


/*
    Macro: circuit_breaker
    Description: Stops pipeline if quality threshold not met.
    
    Usage in pre-hook:
        {{ circuit_breaker(0.95, ref('source'), ref('target'), 'trip_id') }}
*/
{% macro circuit_breaker(min_match_rate, source_model, target_model, key_column) %}
    {% if execute %}
        {% set match_rate = check_reconciliation(source_model, target_model, key_column) %}
        {% if match_rate and match_rate < min_match_rate %}
            {{ exceptions.raise_compiler_error(
                "Circuit breaker triggered! Match rate " ~ match_rate ~ 
                " is below threshold " ~ min_match_rate
            ) }}
        {% endif %}
    {% endif %}
{% endmacro %}


/*
    Macro: detect_late_arrivals
    Description: Identifies records that arrived after their expected processing window.
*/
{% macro is_late_arrival(event_timestamp, extracted_timestamp, threshold_hours=24) %}
    case 
        when {{ extracted_timestamp }} > ({{ event_timestamp }} + interval '{{ threshold_hours }} hours')
        then true
        else false
    end
{% endmacro %}

