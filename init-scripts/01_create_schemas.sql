-- =============================================================================
-- Uber Analytics Data Warehouse - Schema Setup
-- =============================================================================

-- Source schemas (simulating different source systems)
CREATE SCHEMA IF NOT EXISTS source_driver_app;
CREATE SCHEMA IF NOT EXISTS source_rider_app;
CREATE SCHEMA IF NOT EXISTS source_payments;
CREATE SCHEMA IF NOT EXISTS source_support;
CREATE SCHEMA IF NOT EXISTS source_external;

-- Data warehouse schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS integration;
CREATE SCHEMA IF NOT EXISTS dimension;
CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS quality;

-- Development schema
CREATE SCHEMA IF NOT EXISTS dev;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL SCHEMAS TO uber_dw;

