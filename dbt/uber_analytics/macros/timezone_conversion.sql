/*
    Timezone Conversion Macros
    ==========================
    Problem #5: Timezone Handling
*/

/*
    Macro: to_utc
    Description: Converts a local timestamp to UTC based on city timezone.
    
    Usage:
        {{ to_utc('request_timestamp_local', 'city_id') }}
*/
{% macro to_utc(timestamp_column, city_id_column) %}
    (
        {{ timestamp_column }} at time zone (
            select timezone 
            from {{ ref('ref_cities') }} 
            where city_id = {{ city_id_column }}
        )
    ) at time zone 'UTC'
{% endmacro %}


/*
    Macro: from_utc_to_local
    Description: Converts a UTC timestamp to local time based on city.
    
    Usage:
        {{ from_utc_to_local('request_timestamp_utc', 'city_id') }}
*/
{% macro from_utc_to_local(timestamp_column, city_id_column) %}
    (
        {{ timestamp_column }} at time zone 'UTC'
    ) at time zone (
        select timezone 
        from {{ ref('ref_cities') }} 
        where city_id = {{ city_id_column }}
    )
{% endmacro %}


/*
    Macro: get_timezone
    Description: Returns the timezone for a city.
*/
{% macro get_timezone(city_id) %}
    (select timezone from {{ ref('ref_cities') }} where city_id = '{{ city_id }}')
{% endmacro %}

