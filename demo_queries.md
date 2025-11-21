# Demo Queries - Step by Step Validation

This document contains the five key queries to validate each stage of the energy trading pipeline demo.

## 1. Validate No Hive Catalog Exists (After Weather Data)

After the weather data is loaded into Delta Lake, verify that StarRocks doesn't have the external catalog yet:

```sql
-- Connect to StarRocks
mysql -h localhost -P 9030 -u root

-- Check available catalogs
SHOW CATALOGS;
-- Expected: Only 'default_catalog' should be present
```

## 2. Validate Hive Catalog and Weather Data (After Running create_catalogs.sh hive_catalog)

After creating the Hive catalog, verify it exists and can query Delta Lake:

```sql
-- Show all catalogs
SHOW CATALOGS;
-- Expected: Should see 'hive_catalog' in addition to 'default_catalog'

-- Switch to the Hive catalog
SET CATALOG hive_catalog;

-- Show databases
SHOW DATABASES;
-- Expected: Should see 'raw_data' database

-- Query weather data from Delta Lake
SELECT *
FROM raw_data.weather
LIMIT 10;
-- Expected: Should return weather data from Delta Lake
```

## 3. Validate PostgreSQL Catalog (After Running create_catalogs.sh postgres_catalog)

After creating the PostgreSQL catalog, verify it can query the Hive Metastore database:

```sql
-- Show all catalogs
SHOW CATALOGS;
-- Expected: Should see 'postgres_catalog', 'hive_catalog', and 'default_catalog'

-- Switch to PostgreSQL catalog
SET CATALOG postgres_catalog;

-- Show databases
SHOW DATABASES;
-- Expected: Should see 'public' schema

-- Use public schema
USE public;

-- Query Hive metadata tables
SELECT COUNT(*) as db_count FROM DBS;
-- Expected: Count of databases in Hive Metastore

SELECT COUNT(*) as table_count FROM TBLS;
-- Expected: Count of tables in Hive Metastore

-- Sample database names
SELECT * FROM DBS LIMIT 3;
-- Expected: Hive database metadata
```

## 4. Validate Trading Decisions in StarRocks (After Training/Decision Asset)

After the ML model training and AI decision making, verify the data in StarRocks native tables:

```sql
-- Switch back to default catalog
SET CATALOG default_catalog;

-- Use energy_trading database
USE energy_trading;

-- Show tables
SHOW TABLES;
-- Expected: Should see 'trading_decisions' table

-- Query trading decisions
SELECT *
FROM trading_decisions
ORDER BY timestamp DESC
LIMIT 10;
-- Expected: Trading decisions with AI reasoning
```

## 5. Multi-Catalog Joined Query (After multi_catalog_analytics Asset)

The final query that joins data from all three catalogs:

```sql
-- Switch to default catalog to query trading decisions
SET CATALOG default_catalog;
USE energy_trading;

-- Query trading decisions with embedded weather snapshots
SELECT
    id,
    timestamp,
    predicted_price,
    decision,
    avg_temp,
    avg_humidity,
    avg_wind_speed,
    avg_energy_price,
    sample_count
FROM trading_decisions
ORDER BY timestamp DESC
LIMIT 10;
-- Expected: Trading decisions with weather statistics at decision time

-- Or run the live multi-catalog join query with Hive metadata:
SELECT
    td.id as decision_id,
    td.predicted_price,
    td.decision,
    td.timestamp as decision_timestamp,
    td.avg_temp,
    td.avg_humidity,
    td.avg_wind_speed,
    td.avg_energy_price,
    td.sample_count,
    (SELECT COUNT(*) FROM postgres_catalog.public.DBS) as hive_db_count,
    (SELECT COUNT(*) FROM postgres_catalog.public.TBLS) as hive_table_count
FROM default_catalog.energy_trading.trading_decisions td
ORDER BY td.timestamp DESC
LIMIT 10;
-- Expected: Trading decisions from StarRocks native tables with weather snapshots
--   and Hive metadata counts from PostgreSQL catalog, all in one query
```
