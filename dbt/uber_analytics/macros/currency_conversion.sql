/*
    Currency Conversion Macros
    ==========================
    Problem #7: Currency Conversion at Transaction Time
*/

/*
    Macro: convert_to_usd
    Description: Converts an amount to USD using point-in-time exchange rate.
                 Uses the rate from the transaction date, not current date.
    
    Usage:
        {{ convert_to_usd('total_fare_local', 'currency_code', 'transaction_date') }}
*/
{% macro convert_to_usd(amount_column, currency_column, date_column) %}
    case 
        when {{ currency_column }} = 'USD' then {{ amount_column }}
        else {{ amount_column }} * coalesce(
            (
                select exchange_rate 
                from {{ ref('ref_currency_rates') }} rates
                where rates.from_currency = {{ currency_column }}
                  and rates.to_currency = 'USD'
                  and rates.rate_date = {{ date_column }}::date
            ),
            -- Fallback: use most recent rate before the date
            (
                select exchange_rate 
                from {{ ref('ref_currency_rates') }} rates
                where rates.from_currency = {{ currency_column }}
                  and rates.to_currency = 'USD'
                  and rates.rate_date <= {{ date_column }}::date
                order by rates.rate_date desc
                limit 1
            ),
            1.0  -- Ultimate fallback
        )
    end
{% endmacro %}


/*
    Macro: get_exchange_rate
    Description: Gets the exchange rate for a specific date.
    
    Usage:
        {{ get_exchange_rate('EUR', '2024-01-15') }}
*/
{% macro get_exchange_rate(from_currency, rate_date) %}
    (
        select coalesce(
            (
                select exchange_rate 
                from {{ ref('ref_currency_rates') }}
                where from_currency = '{{ from_currency }}'
                  and to_currency = 'USD'
                  and rate_date = '{{ rate_date }}'::date
            ),
            1.0
        )
    )
{% endmacro %}

