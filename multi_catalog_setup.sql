-- Multi-Catalog Setup and Demo SQL Script
-- This script creates PostgreSQL catalog and demonstrates multi-catalog queries

-- Step 1: Create PostgreSQL external catalog
DROP CATALOG IF EXISTS postgres_catalog;

CREATE EXTERNAL CATALOG postgres_catalog
PROPERTIES (
    "type" = "jdbc",
    "jdbc.uri" = "jdbc:postgresql://hive-postgres:5432/metastore",
    "jdbc.user" = "hive",
    "jdbc.password" = "hive",
    "jdbc.driver_class" = "org.postgresql.Driver"
);

-- Step 2: Verify all catalogs
SHOW CATALOGS;

-- Step 3: Explore PostgreSQL catalog
SET CATALOG postgres_catalog;
SHOW DATABASES;
USE metastore;
SHOW TABLES;

-- Show Hive databases registered in metastore
SELECT DB_ID, NAME as database_name, DB_LOCATION_URI 
FROM DBS 
ORDER BY DB_ID;

-- Show Hive tables registered in metastore
SELECT t.TBL_ID, d.NAME as database_name, t.TBL_NAME as table_name, t.TBL_TYPE
FROM TBLS t
JOIN DBS d ON t.DB_ID = d.DB_ID
ORDER BY t.TBL_ID;

-- Step 4: Create multi-catalog analytics table in default catalog
SET CATALOG default_catalog;
USE energy_trading;

DROP TABLE IF EXISTS decision_weather_analytics;

CREATE TABLE decision_weather_analytics (
    decision_id BIGINT,
    predicted_price DECIMAL(10,2),
    should_trade BOOLEAN,
    model_name VARCHAR(255),
    decision_timestamp DATETIME,
    avg_temperature DOUBLE,
    avg_humidity DOUBLE,
    avg_wind_speed DOUBLE,
    avg_energy_price DOUBLE,
    weather_sample_count BIGINT,
    hive_db_count INT,
    hive_table_count INT
) ENGINE=OLAP
DUPLICATE KEY(decision_id)
DISTRIBUTED BY HASH(decision_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

-- Step 5: Insert data from all three catalogs
INSERT INTO decision_weather_analytics
WITH weather_stats AS (
    SELECT 
        AVG(temperature) as avg_temp,
        AVG(humidity) as avg_hum,
        AVG(wind_speed) as avg_wind,
        AVG(energy_price) as avg_price,
        COUNT(*) as sample_count
    FROM hive_catalog.raw_data.weather
),
hive_stats AS (
    SELECT 
        COUNT(DISTINCT DB_ID) as db_count
    FROM postgres_catalog.metastore.DBS
),
table_stats AS (
    SELECT 
        COUNT(DISTINCT TBL_ID) as table_count
    FROM postgres_catalog.metastore.TBLS
)
SELECT 
    td.id as decision_id,
    td.predicted_price,
    td.should_trade,
    td.model_name,
    td.timestamp as decision_timestamp,
    ws.avg_temp,
    ws.avg_hum,
    ws.avg_wind,
    ws.avg_price,
    ws.sample_count,
    hs.db_count,
    ts.table_count
FROM default_catalog.energy_trading.trading_decisions td
CROSS JOIN weather_stats ws
CROSS JOIN hive_stats hs
CROSS JOIN table_stats ts
ORDER BY td.timestamp DESC
LIMIT 100;

-- Step 6: Query the multi-catalog analytics table
SELECT 
    decision_id,
    predicted_price,
    should_trade,
    model_name,
    ROUND(avg_temperature, 2) as avg_temp_celsius,
    ROUND(avg_energy_price, 2) as avg_energy_price_mwh,
    weather_sample_count,
    hive_db_count,
    hive_table_count
FROM decision_weather_analytics
ORDER BY decision_id DESC
LIMIT 10;

-- Step 7: Show cross-catalog join capabilities
-- Query joining all three catalogs in a single query
SELECT 
    'Multi-Catalog Summary' as report_type,
    COUNT(DISTINCT td.id) as total_decisions,
    COUNT(DISTINCT w.temperature) as unique_weather_readings,
    (SELECT COUNT(*) FROM postgres_catalog.metastore.DBS) as hive_databases,
    (SELECT COUNT(*) FROM postgres_catalog.metastore.TBLS) as hive_tables
FROM default_catalog.energy_trading.trading_decisions td
CROSS JOIN hive_catalog.raw_data.weather w;
