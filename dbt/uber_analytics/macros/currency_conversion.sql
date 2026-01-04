/*
================================================================================
Macro: currency_conversion
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#7 CURRENCY CONVERSION (Point-in-Time Exchange Rates)
    Problem: Uber operates globally with trips charged in local currency:
             - NYC: USD
             - London: GBP
             - Tokyo: JPY
             - Mexico City: MXN
             - São Paulo: BRL
    
             Financial reporting needs everything in USD. But exchange rates
             fluctuate DAILY. Using today's rate for a trip from 3 months ago
             is WRONG and will cause:
             - Revenue over/under-statement
             - Incorrect month-over-month comparisons
             - Audit failures
    
    Example:
    | Date       | Trip   | Local Amount | GBP→USD Rate | Correct USD |
    |------------|--------|--------------|--------------|-------------|
    | 2024-01-15 | T001   | £50          | 1.27         | $63.50      |
    | 2024-03-20 | T002   | £50          | 1.25         | $62.50      |
    
    Using today's rate (1.25) for both = WRONG!
    Trip T001 would be $62.50 instead of $63.50 = $1 understated
    
    Multiply by billions of trips = major financial discrepancy.
    
    Business Impact: Misstated revenue, failed audits, incorrect forecasts.

--------------------------------------------------------------------------------

SOLUTION: POINT-IN-TIME RATE LOOKUP
    
    1. Maintain historical exchange rates in `ref_currency_rates`:
       | currency | rate_date  | rate_to_usd |
       | GBP      | 2024-01-15 | 1.27        |
       | GBP      | 2024-01-16 | 1.26        |
       | GBP      | 2024-03-20 | 1.25        |
    
    2. Join on transaction date to get rate THAT DAY:
       ```sql
       select t.*, r.rate_to_usd
       from trips t
       join ref_currency_rates r 
         on t.currency_code = r.currency_code
         and t.transaction_date = r.rate_date
       ```
    
    3. Handle missing dates (weekends, holidays):
       Use most recent rate BEFORE the transaction date
       ```sql
       where r.rate_date <= t.transaction_date
       order by r.rate_date desc
       limit 1
       ```

--------------------------------------------------------------------------------

MACRO USAGE:
    
    {{ convert_to_usd('total_fare_local', 'currency_code', 'transaction_date') }}
    
    Generates:
    ```sql
    coalesce(
        total_fare_local * (
            select rate_to_usd 
            from ref_currency_rates
            where currency_code = trips.currency_code
              and rate_date <= trips.transaction_date
            order by rate_date desc
            limit 1
        ),
        total_fare_local  -- Fallback: assume already USD
    ) as total_fare_local_usd
    ```

================================================================================
*/

{% macro convert_to_usd(amount_column, currency_column, date_column) %}
    /*
        Converts local currency amount to USD using point-in-time exchange rate.
        Falls back to 1:1 if rate not found (assumes USD).
    */
    coalesce(
        {{ amount_column }} * (
            select rate_to_usd 
            from {{ ref('ref_currency_rates') }} rates
            where rates.currency_code = {{ currency_column }}
              and rates.rate_date <= {{ date_column }}::date
            order by rates.rate_date desc
            limit 1
        ),
        -- Fallback: If no rate found, return original (assume USD)
        {{ amount_column }}
    )
{% endmacro %}


{% macro convert_currency(amount_column, from_currency, to_currency, date_column) %}
    /*
        Converts between any two currencies via USD.
        Amount → USD → Target Currency
    */
    coalesce(
        {{ convert_to_usd(amount_column, from_currency, date_column) }} / (
            select rate_to_usd 
            from {{ ref('ref_currency_rates') }} rates
            where rates.currency_code = {{ to_currency }}
              and rates.rate_date <= {{ date_column }}::date
            order by rates.rate_date desc
            limit 1
        ),
        {{ amount_column }}
    )
{% endmacro %}


{% macro get_exchange_rate(currency_column, date_column) %}
    /*
        Returns just the exchange rate (not converted amount).
        Useful when you need the rate as a separate column.
    */
    coalesce(
        (
            select rate_to_usd 
            from {{ ref('ref_currency_rates') }} rates
            where rates.currency_code = {{ currency_column }}
              and rates.rate_date <= {{ date_column }}::date
            order by rates.rate_date desc
            limit 1
        ),
        1.0  -- Default: 1:1 (assume USD)
    )
{% endmacro %}
