/*
    Fiscal Calendar Macros
    ======================
    Problem #8: Fiscal vs Calendar Year
    
    Uber uses different fiscal years by region:
    - US/LATAM: Jan-Dec (standard)
    - UK: Apr-Mar
    - Japan: Apr-Mar
    - Australia: Jul-Jun
*/

/*
    Macro: get_fiscal_year
    Description: Returns fiscal year based on city's fiscal calendar.
    
    Usage:
        {{ get_fiscal_year('transaction_date', 'city_id') }}
*/
{% macro get_fiscal_year(date_column, city_id_column) %}
    case 
        -- Get fiscal year start month for the city
        when extract(month from {{ date_column }}) < (
            select fiscal_year_start_month 
            from {{ ref('ref_cities') }} 
            where city_id = {{ city_id_column }}
        )
        then extract(year from {{ date_column }})::integer
        else extract(year from {{ date_column }})::integer + 
            case 
                when (select fiscal_year_start_month from {{ ref('ref_cities') }} where city_id = {{ city_id_column }}) > 1 
                then 1 
                else 0 
            end
    end
{% endmacro %}


/*
    Macro: get_fiscal_quarter
    Description: Returns fiscal quarter (1-4) based on city's fiscal calendar.
*/
{% macro get_fiscal_quarter(date_column, city_id_column) %}
    case 
        when extract(month from {{ date_column }}) >= (
            select fiscal_year_start_month from {{ ref('ref_cities') }} where city_id = {{ city_id_column }}
        )
        then ((extract(month from {{ date_column }}) - (
            select fiscal_year_start_month from {{ ref('ref_cities') }} where city_id = {{ city_id_column }}
        )) / 3 + 1)::integer
        else ((extract(month from {{ date_column }}) + 12 - (
            select fiscal_year_start_month from {{ ref('ref_cities') }} where city_id = {{ city_id_column }}
        )) / 3 + 1)::integer
    end
{% endmacro %}


/*
    Macro: get_payout_week
    Description: Returns the payout week (Mon-Sun) for driver payouts.
                 Week number and year for settlement purposes.
*/
{% macro get_payout_week(date_column) %}
    -- ISO week (Mon-Sun)
    extract(week from {{ date_column }})
{% endmacro %}

{% macro get_payout_week_start(date_column) %}
    -- Monday of the week
    date_trunc('week', {{ date_column }})::date
{% endmacro %}

{% macro get_payout_week_end(date_column) %}
    -- Sunday of the week
    (date_trunc('week', {{ date_column }}) + interval '6 days')::date
{% endmacro %}

