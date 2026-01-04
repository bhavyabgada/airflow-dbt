/*
================================================================================
Macro: timezone_conversion
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#5 TIMEZONE HANDLING
    Problem: Uber operates in 15+ cities across multiple timezones:
             - New York: America/New_York (UTC-5 / UTC-4 DST)
             - Los Angeles: America/Los_Angeles (UTC-8 / UTC-7 DST)
             - London: Europe/London (UTC+0 / UTC+1 DST)
             - Tokyo: Asia/Tokyo (UTC+9, no DST)
             - Sydney: Australia/Sydney (UTC+10 / UTC+11 DST)
    
             Trip timestamps are stored in LOCAL TIME by the source systems
             (driver app, rider app). But for global analytics we need:
             - UTC for consistent aggregations
             - Local time for city-specific reporting
             - Handle Daylight Saving Time correctly!
    
    Example Problem:
    | City       | Local Time        | UTC Time          | Notes              |
    |------------|-------------------|-------------------|--------------------|
    | NYC        | 2024-03-10 02:30  | Invalid!          | DST skip           |
    | NYC        | 2024-11-03 01:30  | Ambiguous!        | DST fallback       |
    
    March 10, 2024: Clocks skip from 2:00 AM to 3:00 AM (no 2:30 AM exists!)
    November 3, 2024: 1:30 AM happens TWICE (ambiguous)
    
    Business Impact: Wrong "trips per hour" metrics, incorrect peak hour analysis.

--------------------------------------------------------------------------------

SOLUTION: Database-Level Timezone Conversion
    
    PostgreSQL handles timezone conversion with DST automatically:
    ```sql
    timestamp AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC'
    ```
    
    Step 1: Interpret local time as being in source timezone
    Step 2: Convert to UTC
    
    Example:
    '2024-01-15 14:30:00' AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC'
    → '2024-01-15 19:30:00' (5 hours ahead in winter)

--------------------------------------------------------------------------------

MACRO USAGE:
    
    -- Convert to UTC
    {{ convert_to_utc('pickup_timestamp_local', 'city_timezone') }}
    
    -- Convert UTC to local
    {{ convert_to_local('pickup_timestamp_utc', "'America/New_York'") }}
    
    -- Get city timezone from ref table
    {{ get_city_timezone('city_id') }}

--------------------------------------------------------------------------------

TIMEZONE REFERENCE (from ref_cities):
    
    | city_id | timezone              |
    |---------|-----------------------|
    | NYC     | America/New_York      |
    | LAX     | America/Los_Angeles   |
    | CHI     | America/Chicago       |
    | LON     | Europe/London         |
    | PAR     | Europe/Paris          |
    | TYO     | Asia/Tokyo            |
    | SYD     | Australia/Sydney      |
    | MEX     | America/Mexico_City   |
    | SAO     | America/Sao_Paulo     |

================================================================================
*/

{% macro convert_to_utc(timestamp_column, timezone_column) %}
    /*
        Converts local timestamp to UTC.
        Uses database's built-in DST handling.
        
        Example:
        {{ convert_to_utc('request_timestamp_local', 'city_timezone') }}
    */
    {{ timestamp_column }} at time zone {{ timezone_column }} at time zone 'UTC'
{% endmacro %}


{% macro convert_to_local(utc_timestamp_column, timezone) %}
    /*
        Converts UTC timestamp to a specific local timezone.
        
        Example:
        {{ convert_to_local('request_timestamp_utc', "'America/New_York'") }}
        
        Note: Timezone string must be quoted for literal values.
    */
    {{ utc_timestamp_column }} at time zone 'UTC' at time zone {{ timezone }}
{% endmacro %}


{% macro get_city_timezone(city_id_column) %}
    /*
        Looks up timezone for a city from reference data.
        
        Example:
        {{ get_city_timezone('city_id') }} as city_timezone
    */
    (
        select timezone 
        from {{ ref('ref_cities') }} 
        where city_id = {{ city_id_column }}
    )
{% endmacro %}


{% macro is_late_arrival(event_timestamp, extracted_timestamp, hours_threshold) %}
    /*
        Detects late-arriving data by comparing event time vs extraction time.
        
        Example:
        {{ is_late_arrival('request_timestamp', 'extracted_at', 24) }}
        
        Returns TRUE if data arrived more than X hours after the event.
        Used to flag records that need special handling in incremental loads.
    */
    case 
        when extract(epoch from ({{ extracted_timestamp }} - {{ event_timestamp }})) / 3600 > {{ hours_threshold }}
        then true
        else false
    end
{% endmacro %}


{% macro extract_hour_local(utc_timestamp, timezone_column) %}
    /*
        Extracts hour in LOCAL time (for hourly analysis by city).
        
        Example:
        {{ extract_hour_local('request_timestamp_utc', 'city_timezone') }} as request_hour_local
    */
    extract(hour from {{ utc_timestamp }} at time zone 'UTC' at time zone {{ timezone_column }})::integer
{% endmacro %}
