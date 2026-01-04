/*
    Macro: truncate_if_exists
    Description: Safely truncates a table if it exists.
                 Used as a pre-hook for delete+insert incremental strategy.
*/

{% macro truncate_if_exists() %}
    {% set table_exists_query %}
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '{{ this.schema }}' 
            AND table_name = '{{ this.identifier }}'
        )
    {% endset %}
    {% set results = run_query(table_exists_query) %}
    {% if execute and results and results[0][0] %}
        TRUNCATE TABLE {{ this }};
    {% endif %}
{% endmacro %}
