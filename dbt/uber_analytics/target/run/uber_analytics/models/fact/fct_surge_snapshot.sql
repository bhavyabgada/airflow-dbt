
      insert into "uber_analytics"."dev_fact"."fct_surge_snapshot" ("snapshot_key", "snapshot_id", "city_id", "zone_id", "zone_name", "date_key", "snapshot_timestamp", "valid_from", "valid_to", "surge_multiplier", "demand_level", "supply_level", "surge_level", "is_surge_active", "hour_of_day", "day_of_week", "extracted_at", "_fact_loaded_at", "_invocation_id")
    (
        select "snapshot_key", "snapshot_id", "city_id", "zone_id", "zone_name", "date_key", "snapshot_timestamp", "valid_from", "valid_to", "surge_multiplier", "demand_level", "supply_level", "surge_level", "is_surge_active", "hour_of_day", "day_of_week", "extracted_at", "_fact_loaded_at", "_invocation_id"
        from "fct_surge_snapshot__dbt_tmp142402093740"
    )


  