/*
    Macro: audit_columns
    Description: Adds standard audit columns to any model.
                 Use this macro at the end of your SELECT statements.
    
    Usage:
        select
            column1,
            column2,
            {{ audit_columns() }}
        from source_table
*/

{% macro audit_columns() %}
    current_timestamp as _loaded_at,
    '{{ invocation_id }}' as _dbt_invocation_id,
    '{{ this.schema }}' as _target_schema,
    '{{ this.identifier }}' as _target_table,
    '{{ run_started_at }}' as _dbt_run_started_at
{% endmacro %}


/*
    Macro: generate_surrogate_key
    Description: Creates a surrogate key from multiple columns using MD5 hash.
                 Useful for creating unique identifiers in dimensional models.
    
    Usage:
        select
            {{ generate_surrogate_key(['employee_id', 'record_date']) }} as employee_key,
            ...
*/

{% macro generate_surrogate_key(columns) %}
    md5(
        {% for column in columns %}
            coalesce(cast({{ column }} as varchar), '')
            {% if not loop.last %} || '|' || {% endif %}
        {% endfor %}
    )
{% endmacro %}


/*
    Macro: cents_to_dollars
    Description: Converts cents to dollars with proper rounding.
                 Common pattern in financial data processing.
    
    Usage:
        select {{ cents_to_dollars('amount_cents') }} as amount_dollars
*/

{% macro cents_to_dollars(column_name, precision=2) %}
    round(cast({{ column_name }} as numeric) / 100, {{ precision }})
{% endmacro %}


/*
    Macro: safe_divide
    Description: Performs division while handling divide-by-zero scenarios.
    
    Usage:
        select {{ safe_divide('numerator', 'denominator') }} as ratio
*/

{% macro safe_divide(numerator, denominator, default_value=0) %}
    case 
        when {{ denominator }} = 0 or {{ denominator }} is null 
        then {{ default_value }}
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}

