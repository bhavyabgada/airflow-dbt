# 🚗 Uber Analytics Data Warehouse

A production-grade data engineering project demonstrating **34 real-world data engineering challenges** using dbt (Data Build Tool) and Apache Airflow, built on PostgreSQL.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Problems Solved](#problems-solved)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Data Models](#data-models)
- [Key Features](#key-features)
- [Running the Pipeline](#running-the-pipeline)
- [Testing & Validation](#testing--validation)

---

## 🎯 Overview

This project simulates an Uber-like ride-sharing analytics platform, covering:

- **Multi-source data ingestion** from Driver App, Rider App, and Payments systems
- **Multi-currency support** with point-in-time exchange rate conversion
- **Global timezone handling** across 15 cities
- **SCD Type 2 dimensions** for historical tracking
- **Incremental processing** with late-arriving fact handling
- **Production-ready orchestration** with quality gates

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Uber Analytics Pipeline                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                                     │
│  │ Driver  │  │  Rider  │  │Payments │   Source Systems                    │
│  │   App   │  │   App   │  │ System  │                                     │
│  └────┬────┘  └────┬────┘  └────┬────┘                                     │
│       │            │            │                                           │
│       ▼            ▼            ▼                                           │
│  ┌──────────────────────────────────────┐                                  │
│  │         STAGING LAYER (Views)        │   Timezone + PII Masking         │
│  │  stg_trips_driver_app                │                                  │
│  │  stg_trips_rider_app                 │                                  │
│  │  stg_payments                        │                                  │
│  └──────────────────────────────────────┘                                  │
│                      │                                                      │
│                      ▼                                                      │
│  ┌──────────────────────────────────────┐                                  │
│  │      INTEGRATION LAYER (Incr.)       │   Dedup + Late Arrivals          │
│  │  int_trips_unified                   │   + Currency Conversion          │
│  │  int_payments_reconciled             │                                  │
│  └──────────────────────────────────────┘                                  │
│                      │                                                      │
│       ┌──────────────┼──────────────┐                                      │
│       ▼              ▼              ▼                                      │
│  ┌─────────┐  ┌───────────┐  ┌───────────┐                                │
│  │DIMENSION│  │   FACT    │  │   MART    │                                │
│  │  LAYER  │  │   LAYER   │  │   LAYER   │                                │
│  ├─────────┤  ├───────────┤  ├───────────┤                                │
│  │dim_date │  │fct_trips  │  │mart_rev   │                                │
│  │dim_driver│ │fct_accum  │  │mart_driver│                                │
│  │dim_rider │ │fct_earning│  │mart_recon │                                │
│  │dim_geo   │ │fct_surge  │  └───────────┘                                │
│  │bridge_*  │ └───────────┘                                                │
│  │dim_flags │                                                              │
│  └─────────┘                                                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🧩 Problems Solved

This project addresses **34 real-world data engineering challenges**:

### Data Loading & Processing
| # | Problem | Solution | Model(s) |
|---|---------|----------|----------|
| 1 | Late-Arriving Facts | Reprocess with `is_late_arrival` flag | `int_trips_unified`, `fct_trips` |
| 2 | Orphan Records | Validation with reconciliation status | `int_payments_reconciled` |
| 3 | Data Reconciliation | Trip vs Payment matching | `mart_reconciliation` |
| 4 | Duplicate Detection | Row numbering with partition | `stg_trips_rider_app` |
| 5 | Timezone Handling | UTC conversion macros | `timezone_conversion.sql` |
| 6 | Point-in-Time Joins | SCD2 with `valid_from/to` | `dim_driver`, `fct_trips` |
| 7 | Currency Conversion | Transaction-date rates | `currency_conversion.sql` |
| 8 | Fiscal vs Calendar | Multi-region fiscal calendars | `dim_date`, `fiscal_calendar.sql` |

### Dimensional Modeling
| # | Problem | Solution | Model(s) |
|---|---------|----------|----------|
| 9 | Hierarchical Data | Zone → City → Country → Region | `dim_geography` |
| 10 | Multi-Source Merge | Golden record from multiple apps | `int_trips_unified` |
| 11 | SCD Type 2 | dbt snapshots with check strategy | `snap_driver`, `snap_rider` |
| 12 | Conditional Aggregations | CASE statements in aggregations | `mart_revenue_daily` |
| 13 | Backfill Support | Full refresh + incremental modes | All incremental models |
| 14 | Idempotency | Unique keys + merge strategy | All incremental models |

### Advanced Patterns
| # | Problem | Solution | Model(s) |
|---|---------|----------|----------|
| 16 | PII Handling | Conditional masking with var | `stg_drivers`, `stg_riders` |
| 17 | Large Table Joins | Surrogate keys + date partitioning | `fct_trips` |
| 19 | Partitioning Strategy | Date-based partitions | `fct_trips` |
| 20 | Role-Playing Dimensions | Same dim_date for multiple dates | `fct_trips` (request/pickup/dropoff) |
| 21 | Bridge Tables | Many-to-many driver↔vehicle | `bridge_driver_vehicle` |
| 25 | Junk Dimensions | Combined boolean flags | `dim_trip_flags` |
| 26 | Accumulating Snapshots | Lifecycle milestone tracking | `fct_trip_accumulating` |

### Operations & Quality
| # | Problem | Solution | Model(s) |
|---|---------|----------|----------|
| 27 | Data Retention/GDPR | Retention date calculation | `dim_rider` |
| 32 | Cohort Analysis | Signup cohort attributes | `mart_driver_performance` |
| 34 | Circuit Breaker | Quality gate before dims | DAG `quality_gate_1` |

---

## 📁 Project Structure

```
DBT+Airflow/
├── docker-compose.yml          # PostgreSQL data warehouse
├── init-scripts/               # Database initialization
│   └── 01_create_schemas.sql
├── dbt/
│   └── uber_analytics/
│       ├── dbt_project.yml     # Project configuration
│       ├── profiles/           # Connection profiles
│       ├── seeds/              # Reference data (CSV)
│       │   ├── ref_cities.csv
│       │   ├── ref_currencies.csv
│       │   ├── ref_service_types.csv
│       │   ├── ref_currency_rates.csv
│       │   ├── ref_trip_status.csv
│       │   └── source_*.csv    # Sample source data
│       ├── macros/
│       │   ├── currency_conversion.sql
│       │   ├── timezone_conversion.sql
│       │   ├── fiscal_calendar.sql
│       │   ├── data_quality.sql
│       │   └── surrogate_keys.sql
│       ├── snapshots/
│       │   ├── snap_driver.sql
│       │   └── snap_rider.sql
│       └── models/
│           ├── staging/        # Source views
│           ├── integration/    # Cleansed & merged
│           ├── dimension/      # Dimension tables
│           ├── fact/           # Fact tables
│           └── mart/           # Business aggregations
└── airflow/
    └── dags/
        └── uber_analytics_dag.py
```

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- pyenv + pyenv-virtualenv (recommended)

### 1. Start PostgreSQL

```bash
cd /Applications/MAMP/htdocs/DataEngineeringAcademy/DBT+Airflow
docker-compose up -d
```

### 2. Set Up dbt Environment

```bash
# Create virtual environment
pyenv virtualenv 3.11.0 uber_analytics_env
pyenv activate uber_analytics_env

# Install dependencies
pip install dbt-postgres apache-airflow

# Copy profile
cp dbt/uber_analytics/profiles/profiles.yml ~/.dbt/profiles.yml
```

### 3. Run dbt Pipeline

```bash
cd dbt/uber_analytics

# Load reference data
dbt seed

# Run snapshots (SCD Type 2)
dbt snapshot

# Run all models
dbt run

# Run tests
dbt test

# Generate docs
dbt docs generate && dbt docs serve
```

---

## 📊 Data Models

### Seeds (Reference Data)
| Model | Description |
|-------|-------------|
| `ref_cities` | 15 global cities with timezone, currency, fiscal calendar |
| `ref_currencies` | Supported currencies (USD, EUR, GBP, JPY, etc.) |
| `ref_currency_rates` | Historical exchange rates for point-in-time conversion |
| `ref_service_types` | Uber service types (UberX, Black, Pool, Eats) |
| `ref_trip_status` | Trip lifecycle statuses |

### Staging Layer
| Model | Source | Key Features |
|-------|--------|--------------|
| `stg_trips_driver_app` | Driver App | Timezone conversion, earnings |
| `stg_trips_rider_app` | Rider App | Duplicate detection, promo tracking |
| `stg_payments` | Payments | Refund handling, reconciliation prep |
| `stg_drivers` | Driver App | PII masking, change detection hash |
| `stg_riders` | Rider App | GDPR retention date |
| `stg_vehicles` | Driver App | Compliance tracking |
| `stg_surge_snapshots` | Surge Service | 2-minute validity windows |

### Integration Layer
| Model | Key Features |
|-------|--------------|
| `int_trips_unified` | Multi-source merge, dedup, currency conversion, late arrival handling |
| `int_payments_reconciled` | Orphan detection, net amount calculation |

### Dimension Layer
| Model | Type | Key Features |
|-------|------|--------------|
| `dim_date` | Role-Playing | Calendar + 3 fiscal calendars (US, UK, AU) |
| `dim_geography` | Hierarchy | Zone → City → Country → Region |
| `dim_driver` | SCD Type 2 | Point-in-time lookups |
| `dim_rider` | SCD Type 2 | GDPR deletion eligibility |
| `bridge_driver_vehicle` | Bridge | Many-to-many with allocation weights |
| `dim_trip_flags` | Junk | 8 boolean flags combined |

### Fact Layer
| Model | Type | Grain | Key Measures |
|-------|------|-------|--------------|
| `fct_trips` | Transaction | 1 row per trip | fare_usd, driver_earnings_usd, duration |
| `fct_trip_accumulating` | Accumulating | 1 row per trip | Milestone timestamps, lifecycle duration |
| `fct_driver_earnings` | Periodic Snapshot | Driver × Week | Weekly earnings, efficiency metrics |
| `fct_surge_snapshot` | Periodic Snapshot | Zone × 2min | Surge multiplier, supply/demand |

### Mart Layer
| Model | Purpose | Key Metrics |
|-------|---------|-------------|
| `mart_revenue_daily` | Finance | Gross bookings, net revenue, take rate |
| `mart_driver_performance` | Operations | Engagement status, performance tier |
| `mart_reconciliation` | Audit | Match rate, circuit breaker flag |

---

## ✨ Key Features

### Multi-Currency Support
```sql
-- Currency conversion at transaction date
{{ convert_to_usd('total_fare_local', 'currency_code', 'transaction_date') }}
```

### Point-in-Time SCD2 Joins
```sql
-- Join to driver at time of trip
left join dim_driver d on t.driver_id = d.driver_id
    and t.request_timestamp >= d.valid_from
    and (t.request_timestamp < d.valid_to or d.valid_to is null)
```

### Junk Dimension Pattern
```sql
-- Combine 8 boolean flags into single key
left join dim_trip_flags tf on 
    tf.is_surge = (t.surge_multiplier > 1.0)
    and tf.is_pool = (t.service_type_id = 'UBER_POOL')
    and tf.has_tip = (t.tips_usd > 0)
    ...
```

### Late-Arriving Facts
```sql
-- Always reprocess late arrivals
{% if is_incremental() %}
    where extracted_at > (select max(extracted_at) from {{ this }})
       or is_late_arrival = true
{% endif %}
```

### Circuit Breaker Pattern
```python
# In Airflow DAG - stop pipeline if quality threshold not met
def check_quality_gate(**context):
    if match_rate < 0.99:
        return 'quality_failed'
    return 'dimensions.start_dimensions'
```

---

## 🔄 Running the Pipeline

### With dbt Commands

```bash
# Full refresh (backfill)
dbt run --full-refresh

# Incremental run with execution date
dbt run --vars '{"execution_date": "2024-01-15"}'

# Run specific layer
dbt run --select staging.*
dbt run --select +fct_trips  # fct_trips and all upstream
```

### With Airflow

```bash
# Start Airflow
export AIRFLOW_HOME=/path/to/airflow
airflow standalone

# Trigger DAG
airflow dags trigger uber_analytics_pipeline

# Backfill
airflow dags backfill uber_analytics_pipeline \
    --start-date 2024-01-01 \
    --end-date 2024-01-15
```

---

## ✅ Testing & Validation

### Built-in Tests
- `unique` / `not_null` on all keys
- `accepted_values` for status columns
- `relationships` for referential integrity

### Custom Tests Available
```yaml
columns:
  - name: total_fare_usd
    tests:
      - not_null
      - dbt_utils.expression_is_true:
          expression: ">= 0"
```

### Data Quality Dashboard
The `mart_reconciliation` model provides:
- Trip vs Payment match rate
- Orphan payment count
- Circuit breaker trigger status

---

## 📚 Additional Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [Apache Airflow](https://airflow.apache.org/)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/)

---

## 📝 License

This project is for educational purposes as part of the Data Engineering Academy.

---

**Built with ❤️ for Data Engineers**
