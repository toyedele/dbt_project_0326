-- =============================================================
-- SNOWFLAKE PROVISIONING SCRIPT
-- This is a one-off set up script to create the necessary resources for an ELT pipeline and a medallion architecture.
-- It is to be run in the Snowflake UI or via SnowSQL by an ACCOUNTADMIN user. After running this, you can switch to a less-privileged role for day-to-day work.
-- Pipeline: CSV → Bronze → Silver → Gold → Reader Account
-- =============================================================

USE ROLE ACCOUNTADMIN;

-- ─────────────────────────────────────────────────────────────
-- 1. Databases (Raw and Analytics)
-- ─────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS RAW  COMMENT = 'Raw ingested data from sources';
CREATE DATABASE IF NOT EXISTS ANALYTICS  COMMENT = 'Databases for staging, transformation, and marts following a medallion architecture';

-- ─────────────────────────────────────────────────────────────
-- 2. Schemas for Raw and Analytics Database (Events, User, Transactions, product, Finance)
-- ─────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS RAW.EVENTS COMMENT = 'Events related data such as marketing clicks';
CREATE SCHEMA IF NOT EXISTS RAW.USER COMMENT = 'Customer related data such as demographics and marketing subscription information';
CREATE SCHEMA IF NOT EXISTS RAW.TRANSACTIONS COMMENT = 'Transactions related data such as orders, payments and refunds';
CREATE SCHEMA IF NOT EXISTS RAW.PRODUCT COMMENT = 'Product related data such as order items and product information';
CREATE SCHEMA IF NOT EXISTS RAW.FINANCE COMMENT = 'Finance related data such as FX Rates';

CREATE SCHEMA IF NOT EXISTS ANALYTICS.ANALYTICS COMMENT = 'For Marts dataset used for reporting and BI';

-- ─────────────────────────────────────────────────────────────
-- 3. File Stages (internal stages for ingesting data into the Raw database)
-- ─────────────────────────────────────────────────────────────
CREATE STAGE IF NOT EXISTS RAW.EVENTS.EVENTS_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    COMMENT = 'Internal stage for ingesting events data';

CREATE STAGE IF NOT EXISTS RAW.USER.USER_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    COMMENT = 'Internal stage for ingesting user data';

CREATE STAGE IF NOT EXISTS RAW.TRANSACTIONS.TRANSACTIONS_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    COMMENT = 'Internal stage for ingesting transactions data';

CREATE STAGE IF NOT EXISTS RAW.PRODUCT.PRODUCT_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    COMMENT = 'Internal stage for ingesting product data';

CREATE STAGE IF NOT EXISTS RAW.FINANCE.FINANCE_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    COMMENT = 'Internal stage for ingesting finance data';

-- ─────────────────────────────────────────────────────────────
-- 4. Warehouses (ingestion, transformation, reporting)
-- ─────────────────────────────────────────────────────────────

CREATE WAREHOUSE IF NOT EXISTS INGESTION_WH
    WAREHOUSE_SIZE    = 'X-SMALL'
    AUTO_SUSPEND      = 60
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used by ingestion scripts to load data';

CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WAREHOUSE_SIZE    = 'SMALL'
    AUTO_SUSPEND      = 120
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used by dbt for data transformations';

CREATE WAREHOUSE IF NOT EXISTS REPORTING_WH
    WAREHOUSE_SIZE    = 'X-SMALL'
    AUTO_SUSPEND      = 60
    AUTO_RESUME       = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used by reader accounts and BI tools';

-- ─────────────────────────────────────────────────────────────
-- 5. ROLES
-- ─────────────────────────────────────────────────────────────

CREATE ROLE IF NOT EXISTS DATA_LOADER COMMENT = 'Role for loading data into the Raw database';
CREATE ROLE IF NOT EXISTS DBT_TRANSFORMER COMMENT = 'Role for running dbt transformations in the Analytics database';
CREATE ROLE IF NOT EXISTS REPORTER COMMENT = 'Role for read-only access to the Analytics database for reporting and BI';

-- ─────────────────────────────────────────────────────────────
-- 6. Role Grants
-- ─────────────────────────────────────────────────────────────
GRANT USAGE ON WAREHOUSE INGESTION_WH TO ROLE DATA_LOADER;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE DBT_TRANSFORMER;
GRANT USAGE ON WAREHOUSE REPORTING_WH TO ROLE REPORTER;
GRANT USAGE ON WAREHOUSE REPORTING_WH TO ROLE DBT_TRANSFORMER; -- dbt also needs access for documents generation

-- Role hierarchy: SYSADMIN owns all the roles and can switch to them as needed
GRANT ROLE DATA_LOADER   TO ROLE SYSADMIN;
GRANT ROLE DBT_TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE REPORTER        TO ROLE SYSADMIN;

-- Ingestion
GRANT USAGE ON DATABASE RAW TO ROLE DATA_LOADER;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE RAW TO ROLE DATA_LOADER;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE RAW TO ROLE DATA_LOADER;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE RAW TO ROLE DATA_LOADER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE RAW TO ROLE DATA_LOADER;
GRANT ALL PRIVILEGES ON ALL STAGES IN DATABASE RAW TO ROLE DATA_LOADER;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN DATABASE RAW TO ROLE DATA_LOADER;

-- Transformation
GRANT USAGE ON DATABASE RAW TO ROLE DBT_TRANSFORMER;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE RAW TO ROLE DBT_TRANSFORMER;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE RAW TO ROLE DBT_TRANSFORMER;
GRANT SELECT ON ALL TABLES IN DATABASE RAW TO ROLE DBT_TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN DATABASE RAW TO ROLE DBT_TRANSFORMER;

GRANT USAGE, CREATE SCHEMA ON DATABASE ANALYTICS TO ROLE DBT_TRANSFORMER;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE ANALYTICS TO ROLE DBT_TRANSFORMER;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE ANALYTICS TO ROLE DBT_TRANSFORMER;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE ANALYTICS TO ROLE DBT_TRANSFORMER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE ANALYTICS TO ROLE DBT_TRANSFORMER;

-- Reporting
GRANT USAGE ON DATABASE ANALYTICS TO ROLE REPORTER;
GRANT USAGE ON SCHEMA ANALYTICS.ANALYTICS TO ROLE REPORTER;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS.ANALYTICS TO ROLE REPORTER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS.ANALYTICS TO ROLE REPORTER;

-- ─────────────────────────────────────────────────────────────
-- 7.  User creation and role assigning 
-- ─────────────────────────────────────────────────────────────

CREATE USER IF NOT EXISTS LOADER
    DEFAULT_ROLE     = DATA_LOADER
    DEFAULT_WAREHOUSE = INGESTION_WH
    COMMENT = 'Service user for data ingestion';

CREATE USER IF NOT EXISTS DBT
    DEFAULT_ROLE     = DBT_TRANSFORMER
    DEFAULT_WAREHOUSE = TRANSFORM_WH
    COMMENT = 'Service user for dbt transformations';

GRANT ROLE DATA_LOADER TO USER LOADER;
GRANT ROLE DBT_TRANSFORMER TO USER DBT;

USE ROLE DBT_TRANSFORMER;
USE WAREHOUSE TRANSFORM_WH;