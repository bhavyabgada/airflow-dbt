/*
================================================================================
Macro: fiscal_calendar
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#8 FISCAL VS CALENDAR YEAR
    Problem: Different countries and business units use different fiscal years:
             
             | Region    | Fiscal Year Start | Example: Q1 Dates     |
             |-----------|-------------------|------------------------|
             | US/LATAM  | January 1         | Jan 1 - Mar 31        |
             | UK/Japan  | April 1           | Apr 1 - Jun 30        |
             | Australia | July 1            | Jul 1 - Sep 30        |
             | India     | April 1           | Apr 1 - Jun 30        |
             
             A date like "March 15, 2024" falls into different fiscal quarters:
             - US: FY2024 Q1
             - UK: FY2024 Q4 (year ending March 2024)
             - AU: FY2024 Q3
             
             Financial reports MUST use correct fiscal year for each region.
    
    Business Impact: 
    - Wrong quarterly targets
    - Incorrect YoY comparisons
    - Audit failures for regional reports
    - Confused stakeholders

--------------------------------------------------------------------------------

SOLUTION: Multiple Fiscal Calendars
    
    1. Store fiscal_calendar_type per city/region in ref_cities:
       | city_id | fiscal_calendar_type |
       | NYC     | US                   |
       | LON     | UK                   |
       | SYD     | AU                   |
    
    2. dim_date includes columns for ALL fiscal calendars:
       - fiscal_year_us, fiscal_quarter_us
       - fiscal_year_uk, fiscal_quarter_uk
       - fiscal_year_au, fiscal_quarter_au
    
    3. Reports use appropriate column based on region:
       ```sql
       SELECT 
           CASE city_fiscal_type
               WHEN 'US' THEN d.fiscal_quarter_us
               WHEN 'UK' THEN d.fiscal_quarter_uk
               WHEN 'AU' THEN d.fiscal_quarter_au
           END as fiscal_quarter
       FROM fct_trips t
       JOIN dim_date d ON t.trip_date_key = d.date_key
       ```

--------------------------------------------------------------------------------

FISCAL YEAR CALCULATION LOGIC:

    US Fiscal (Jan-Dec):
    - Same as calendar year
    - fiscal_year = calendar_year
    - fiscal_quarter = ceiling(month / 3)
    
    UK Fiscal (Apr-Mar):
    - Year "rolls over" in April
    - If month >= 4: fiscal_year = calendar_year + 1
    - If month < 4: fiscal_year = calendar_year
    - Q1 = Apr-Jun, Q2 = Jul-Sep, Q3 = Oct-Dec, Q4 = Jan-Mar
    
    AU Fiscal (Jul-Jun):
    - Year "rolls over" in July  
    - If month >= 7: fiscal_year = calendar_year + 1
    - If month < 7: fiscal_year = calendar_year
    - Q1 = Jul-Sep, Q2 = Oct-Dec, Q3 = Jan-Mar, Q4 = Apr-Jun

================================================================================
*/

{% macro get_fiscal_year(date_column, fiscal_type) %}
    /*
        Returns fiscal year for a given date based on fiscal calendar type.
        
        Usage:
        {{ get_fiscal_year('order_date', 'UK') }}
    */
    case '{{ fiscal_type }}'
        when 'US' then extract(year from {{ date_column }})::integer
        when 'UK' then 
            case 
                when extract(month from {{ date_column }}) >= 4 
                then extract(year from {{ date_column }})::integer + 1
                else extract(year from {{ date_column }})::integer
            end
        when 'AU' then 
            case 
                when extract(month from {{ date_column }}) >= 7 
                then extract(year from {{ date_column }})::integer + 1
                else extract(year from {{ date_column }})::integer
            end
        else extract(year from {{ date_column }})::integer  -- Default to calendar
    end
{% endmacro %}


{% macro get_fiscal_quarter(date_column, fiscal_type) %}
    /*
        Returns fiscal quarter (1-4) for a given date based on fiscal calendar type.
        
        Usage:
        {{ get_fiscal_quarter('order_date', 'UK') }}
    */
    case '{{ fiscal_type }}'
        when 'US' then 
            -- Standard: Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec
            ceiling(extract(month from {{ date_column }}) / 3.0)::integer
        when 'UK' then 
            -- UK Fiscal: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
            case 
                when extract(month from {{ date_column }}) >= 4 
                then ceiling((extract(month from {{ date_column }}) - 3) / 3.0)::integer
                else ceiling((extract(month from {{ date_column }}) + 9) / 3.0)::integer
            end
        when 'AU' then 
            -- AU Fiscal: Q1=Jul-Sep, Q2=Oct-Dec, Q3=Jan-Mar, Q4=Apr-Jun
            case 
                when extract(month from {{ date_column }}) >= 7 
                then ceiling((extract(month from {{ date_column }}) - 6) / 3.0)::integer
                else ceiling((extract(month from {{ date_column }}) + 6) / 3.0)::integer
            end
        else ceiling(extract(month from {{ date_column }}) / 3.0)::integer  -- Default
    end
{% endmacro %}


{% macro get_fiscal_month(date_column, fiscal_type) %}
    /*
        Returns fiscal month (1-12) for a given date based on fiscal calendar type.
        Month 1 = first month of fiscal year.
        
        Usage:
        {{ get_fiscal_month('order_date', 'UK') }}
    */
    case '{{ fiscal_type }}'
        when 'US' then 
            extract(month from {{ date_column }})::integer
        when 'UK' then 
            -- UK: April = Month 1, March = Month 12
            case 
                when extract(month from {{ date_column }}) >= 4 
                then extract(month from {{ date_column }})::integer - 3
                else extract(month from {{ date_column }})::integer + 9
            end
        when 'AU' then 
            -- AU: July = Month 1, June = Month 12
            case 
                when extract(month from {{ date_column }}) >= 7 
                then extract(month from {{ date_column }})::integer - 6
                else extract(month from {{ date_column }})::integer + 6
            end
        else extract(month from {{ date_column }})::integer  -- Default
    end
{% endmacro %}


{% macro fiscal_year_label(date_column, fiscal_type) %}
    /*
        Returns human-readable fiscal year label.
        UK/AU use FYyyyy format (year is the END year of fiscal period).
        
        Example:
        - UK fiscal year Apr 2023 - Mar 2024 = "FY2024"
        - AU fiscal year Jul 2023 - Jun 2024 = "FY2024"
        
        Usage:
        {{ fiscal_year_label('order_date', 'UK') }}
    */
    'FY' || {{ get_fiscal_year(date_column, fiscal_type) }}::varchar
{% endmacro %}
