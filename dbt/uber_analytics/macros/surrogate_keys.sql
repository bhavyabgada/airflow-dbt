/*
    Surrogate Key Macros
    ====================
    Generate consistent surrogate keys for dimensional modeling.
*/

/*
    Macro: generate_surrogate_key
    Description: Creates an MD5 hash surrogate key from multiple columns.
    
    Usage:
        {{ generate_surrogate_key(['driver_id', 'effective_date']) }}
*/
{% macro generate_surrogate_key(columns) %}
    md5(
        {% for column in columns %}
            coalesce(cast({{ column }} as varchar), '_NULL_')
            {% if not loop.last %} || '|' || {% endif %}
        {% endfor %}
    )
{% endmacro %}


/*
    Macro: generate_scd2_key
    Description: Creates a surrogate key for SCD Type 2 records.
                 Includes the natural key and effective date.
*/
{% macro generate_scd2_key(natural_key, effective_date) %}
    md5(
        coalesce(cast({{ natural_key }} as varchar), '_NULL_') || '|' ||
        coalesce(cast({{ effective_date }} as varchar), '_NULL_')
    )
{% endmacro %}


/*
    Macro: generate_junk_dim_key  
    Description: Creates a key for junk dimension combinations.
    
    Problem #25: Junk Dimensions
*/
{% macro generate_junk_dim_key(flag_columns) %}
    md5(
        {% for column in flag_columns %}
            coalesce(cast({{ column }} as varchar), 'N')
            {% if not loop.last %} || {% endif %}
        {% endfor %}
    )
{% endmacro %}

