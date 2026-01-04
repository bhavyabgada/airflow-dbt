/*
    Data Quality Macros
    ===================
    Collection of macros for data quality checks and validations.
*/


/*
    Macro: check_null_percentage
    Description: Returns the percentage of null values in a column.
    
    Usage:
        {% set null_pct = check_null_percentage(ref('my_model'), 'column_name') %}
*/

{% macro check_null_percentage(relation, column_name) %}
    {% set query %}
        select 
            round(
                100.0 * sum(case when {{ column_name }} is null then 1 else 0 end) / count(*),
                2
            ) as null_percentage
        from {{ relation }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {{ return(results.columns[0].values()[0]) }}
    {% endif %}
{% endmacro %}


/*
    Macro: check_duplicate_count
    Description: Returns the count of duplicate values for given columns.
*/

{% macro check_duplicate_count(relation, columns) %}
    {% set columns_csv = columns | join(', ') %}
    {% set query %}
        select count(*) - count(distinct {{ columns_csv }}) as duplicate_count
        from {{ relation }}
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {{ return(results.columns[0].values()[0]) }}
    {% endif %}
{% endmacro %}


/*
    Macro: compare_row_counts
    Description: Compares row counts between two models.
                 Returns the difference (model1 - model2).
*/

{% macro compare_row_counts(model1, model2) %}
    {% set query %}
        select 
            (select count(*) from {{ model1 }}) - 
            (select count(*) from {{ model2 }}) as row_diff
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {{ return(results.columns[0].values()[0]) }}
    {% endif %}
{% endmacro %}


/*
    Macro: get_column_stats
    Description: Returns basic statistics for a numeric column.
*/

{% macro get_column_stats(relation, column_name) %}
    {% set query %}
        select 
            min({{ column_name }}) as min_value,
            max({{ column_name }}) as max_value,
            avg({{ column_name }}) as avg_value,
            count(*) as total_count,
            count({{ column_name }}) as non_null_count
        from {{ relation }}
    {% endset %}
    {{ return(run_query(query)) }}
{% endmacro %}


/*
    Macro: flag_outliers
    Description: Creates a column flagging statistical outliers using IQR method.
    
    Usage in model:
        select
            *,
            {{ flag_outliers('salary', 1.5) }} as is_salary_outlier
        from employees
*/

{% macro flag_outliers(column_name, iqr_multiplier=1.5) %}
    case 
        when {{ column_name }} < (
            percentile_cont(0.25) within group (order by {{ column_name }}) over() -
            {{ iqr_multiplier }} * (
                percentile_cont(0.75) within group (order by {{ column_name }}) over() -
                percentile_cont(0.25) within group (order by {{ column_name }}) over()
            )
        ) then true
        when {{ column_name }} > (
            percentile_cont(0.75) within group (order by {{ column_name }}) over() +
            {{ iqr_multiplier }} * (
                percentile_cont(0.75) within group (order by {{ column_name }}) over() -
                percentile_cont(0.25) within group (order by {{ column_name }}) over()
            )
        ) then true
        else false
    end
{% endmacro %}

