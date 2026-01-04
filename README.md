# Employee Analytics Data Warehouse

A production-ready data engineering project demonstrating **dbt** and **Apache Airflow** integration with PostgreSQL. This project showcases advanced data engineering capabilities including incremental processing strategies, SCD Type 2 implementations, data quality testing, and orchestration best practices.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AIRFLOW ORCHESTRATION                              │
│  ┌─────────┐  ┌──────────┐  ┌────────┐  ┌─────────┐  ┌──────┐  ┌─────────┐ │
│  │  Seeds  │→ │ Snapshots │→ │ Source │→ │ Staging │→ │ Mart │→ │  Tests  │ │
│  └─────────┘  └──────────┘  └────────┘  └─────────┘  └──────┘  └─────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DBT TRANSFORMATIONS                             │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         SOURCE LAYER                                 │   │
│  │  • stg_employees_raw (Full refresh table)                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      INTERMEDIATE LAYER                              │   │
│  │  • int_employees_merged (MERGE strategy - upsert)                   │   │
│  │  • int_employees_snapshot (DELETE+INSERT strategy)                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                          MART LAYER                                  │   │
│  │  • fct_employee_history (APPEND strategy - historical fact)         │   │
│  │  • dim_departments (Reference dimension)                            │   │
│  │  • dim_job_titles (Reference dimension)                             │   │
│  │  • dbt_audit_log (Observability)                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      SNAPSHOT (SCD Type 2)                           │   │
│  │  • employee_scd (Historical change tracking)                        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              POSTGRESQL                                      │
│                          (Docker Container)                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Features Demonstrated

### Data Engineering Capabilities

| Feature | Implementation | Description |
|---------|---------------|-------------|
| **Incremental Strategies** | MERGE, DELETE+INSERT, APPEND | Different strategies for different use cases |
| **SCD Type 2** | dbt Snapshots | Track historical changes with valid_from/valid_to |
| **Data Quality Tests** | Schema + Custom Tests | Ensure data integrity at every layer |
| **Custom Macros** | Audit, Logging, Quality | Reusable SQL components |
| **Source Freshness** | Monitoring | Alert when source data is stale |
| **Reference Data** | Seeds | Manage dimension data via CSV |
| **Documentation** | Auto-generated | Full lineage and column descriptions |

### Orchestration Capabilities

| Feature | Implementation | Description |
|---------|---------------|-------------|
| **Task Groups** | Logical grouping | Organized DAG structure |
| **Data Quality Gates** | Branch operators | Stop pipeline on quality failures |
| **Notifications** | Callbacks | Slack/email on failure |
| **Retries** | Exponential backoff | Handle transient failures |
| **Parameterization** | Execution date vars | Backfill support |
| **SLA Monitoring** | Built-in | Track pipeline performance |

## Project Structure

```
DBT+Airflow/
├── docker-compose.yml              # PostgreSQL container
├── README.md                       # This file
├── .gitignore                      # Git ignore rules
│
├── dbt/
│   ├── demo_dbt_env/               # Python virtual environment
│   └── employee_analytics/         # dbt project
│       ├── dbt_project.yml         # Project configuration
│       ├── .sqlfluff               # SQL linting rules
│       ├── .sqlfluffignore         # Linting exclusions
│       │
│       ├── models/
│       │   ├── source/
│       │   │   ├── stg_employees_raw.sql
│       │   │   └── sources.yml     # Source definitions + freshness
│       │   ├── staging/
│       │   │   ├── int_employees_merged.sql
│       │   │   ├── int_employees_snapshot.sql
│       │   │   └── schema.yml      # Column docs + tests
│       │   ├── mart/
│       │   │   ├── fct_employee_history.sql
│       │   │   ├── dim_departments.sql
│       │   │   ├── dim_job_titles.sql
│       │   │   └── schema.yml
│       │   └── audit/
│       │       ├── dbt_audit_log.sql
│       │       └── schema.yml
│       │
│       ├── snapshots/
│       │   └── employee_scd.sql    # SCD Type 2 implementation
│       │
│       ├── seeds/
│       │   ├── departments.csv     # Reference data
│       │   ├── job_titles.csv
│       │   └── schema.yml
│       │
│       ├── macros/
│       │   ├── audit_columns.sql   # Audit column helpers
│       │   ├── logging.sql         # Run logging macros
│       │   ├── data_quality.sql    # Quality check helpers
│       │   └── delete_script.sql   # Truncate helper
│       │
│       └── tests/
│           └── generic/
│               ├── test_positive_value.sql
│               ├── test_not_empty_string.sql
│               ├── test_date_in_past.sql
│               ├── test_row_count_minimum.sql
│               └── test_referential_integrity.sql
│
└── airflow/
    ├── demo_airflow_env/           # Python virtual environment
    ├── airflow.cfg                 # Airflow configuration
    └── dags/
        └── dbt_dag.py              # Production-ready DAG
```

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- macOS/Linux (Windows with WSL)

### 1. Start PostgreSQL

```bash
cd /Applications/MAMP/htdocs/DataEngineeringAcademy/DBT+Airflow
docker-compose up -d
```

### 2. Set Up dbt

```bash
cd dbt
python3 -m venv demo_dbt_env
source demo_dbt_env/bin/activate
pip install dbt-postgres

# Configure profile
mkdir -p ~/.dbt
cat << 'EOF' > ~/.dbt/profiles.yml
employee_analytics:
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: dw_user
      pass: dw_password
      dbname: dw
      schema: dev
      threads: 4
    prod:
      type: postgres
      host: localhost
      port: 5432
      user: dw_user
      pass: dw_password
      dbname: dw
      schema: prod
      threads: 4
  target: dev
EOF

# Verify connection
dbt debug --project-dir employee_analytics
```

### 3. Run dbt Models

```bash
source demo_dbt_env/bin/activate
cd employee_analytics

# Load seeds (reference data)
dbt seed

# Run snapshots (SCD Type 2)
dbt snapshot

# Run all models
dbt run --vars '{execution_date: 2022-09-02}'

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

### 4. Set Up Airflow

```bash
cd ../airflow
python3 -m venv demo_airflow_env
source demo_airflow_env/bin/activate
pip install "apache-airflow==2.10.0"

export AIRFLOW_HOME=$(pwd)
airflow db migrate
airflow standalone
```

Access Airflow UI at: http://localhost:8091

## Incremental Strategies Explained

### 1. MERGE Strategy (`int_employees_merged`)
- **Use Case**: Dimension tables needing upsert behavior
- **Behavior**: Updates existing rows, inserts new ones
- **Key Config**: `unique_key='employee_id'`

### 2. DELETE+INSERT Strategy (`int_employees_snapshot`)
- **Use Case**: Full replacement of matching records
- **Behavior**: Deletes then inserts based on unique key
- **Key Config**: `incremental_strategy='delete+insert'`

### 3. APPEND Strategy (`fct_employee_history`)
- **Use Case**: Fact tables with historical tracking
- **Behavior**: Only inserts new records
- **Key Config**: No unique_key, filters by execution_date

## Custom Tests

| Test | Purpose | Usage |
|------|---------|-------|
| `positive_value` | Ensure numbers > 0 | IDs, amounts |
| `not_empty_string` | No empty strings | Names, codes |
| `date_in_past` | No future dates | DOB, hire dates |
| `row_count_minimum` | Min row threshold | Critical tables |
| `referential_integrity` | FK validation | Dimension lookups |

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AIRFLOW_HOME` | Airflow home directory | `./airflow` |
| `DBT_PROFILES_DIR` | dbt profiles location | `~/.dbt` |

## Commands Reference

```bash
# dbt Commands
dbt run                          # Run all models
dbt run --select stg_*           # Run source models only
dbt run --select tag:daily       # Run by tag
dbt test                         # Run all tests
dbt test --select fct_*          # Test specific models
dbt snapshot                     # Run SCD snapshots
dbt seed                         # Load seed data
dbt docs generate                # Generate documentation
dbt source freshness             # Check source freshness

# Airflow Commands
airflow standalone               # Start all services
airflow dags list                # List all DAGs
airflow dags trigger employee_analytics_pipeline  # Trigger DAG
airflow tasks test employee_analytics_pipeline stg_employees_raw 2022-09-01
```

## Data Quality Checks

The project implements multiple layers of data quality:

1. **Schema Tests**: Defined in `schema.yml` files
   - `not_null`, `unique`, `accepted_values`, `relationships`

2. **Custom Generic Tests**: Reusable test definitions
   - Located in `tests/generic/`

3. **Source Freshness**: Monitor data staleness
   - Configured in `sources.yml`

4. **Audit Logging**: Track all dbt runs
   - `dbt_audit_log` model captures execution metadata

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run linting: `sqlfluff lint models/`
4. Run tests: `dbt test`
5. Submit a pull request

## License

MIT License - See LICENSE file for details.

---

**Author**: Data Engineering Team  
**Version**: 2.0.0  
**Last Updated**: January 2026

