-- ============================================================================
-- Multi-Connector Showcase Query
-- Demonstrates StarRocks accessing 3 different storage technologies
-- through a single MySQL endpoint
-- ============================================================================

-- 1. Show all available catalogs and their types
SELECT 
    'Available Catalogs' as info_type,
    NULL as catalog,
    NULL as database_name,
    NULL as table_name,
    NULL as storage_technology,
    NULL as location
FROM dual

UNION ALL

SELECT 
    'Catalog List' as info_type,
    Catalog as catalog,
    Type as database_name,
    Comment as table_name,
    NULL as storage_technology,
    NULL as location
FROM (
    SHOW CATALOGS
) catalogs;

-- ============================================================================
-- 2. StarRocks Native OLAP Storage (default_catalog)
-- ============================================================================

SET CATALOG default_catalog;

SELECT 
    'StarRocks Native' as info_type,
    'default_catalog' as catalog,
    'energy_trading' as database_name,
    'trading_decisions' as table_name,
    'StarRocks OLAP' as storage_technology,
    'Local StarRocks Storage' as location
FROM dual

UNION ALL

SELECT 
    'Table Schema',
    'default_catalog',
    'energy_trading',
    CONCAT('Column: ', Field) as table_name,
    Type as storage_technology,
    CONCAT('Key: ', `Key`) as location
FROM (
    DESC energy_trading.trading_decisions
) schema_info

UNION ALL

SELECT 
    'Sample Data',
    'default_catalog',
    'energy_trading',
    CONCAT('Decision #', CAST(id AS STRING)) as table_name,
    CONCAT('Price: $', CAST(predicted_price AS STRING)) as storage_technology,
    CONCAT('Trade: ', CAST(decision AS STRING)) as location
FROM energy_trading.trading_decisions
LIMIT 3;

-- ============================================================================
-- 3. Delta Lake via Hive Metastore (hive_catalog)
-- ============================================================================

SET CATALOG hive_catalog;

SELECT 
    'Delta Lake Storage' as info_type,
    'hive_catalog' as catalog,
    'raw_data' as database_name,
    'weather' as table_name,
    'Delta Lake (Parquet)' as storage_technology,
    'Azure Blob Storage (ABFSS)' as location
FROM dual

UNION ALL

SELECT 
    'Database List',
    'hive_catalog',
    Database as database_name,
    NULL as table_name,
    'Hive Metastore' as storage_technology,
    NULL as location
FROM (
    SHOW DATABASES
) dbs
WHERE Database NOT IN ('information_schema', 'default')

UNION ALL

SELECT 
    'Table List',
    'hive_catalog',
    'raw_data' as database_name,
    Tables_in_raw_data as table_name,
    'External Delta Table' as storage_technology,
    'Registered in Hive' as location
FROM (
    USE raw_data;
    SHOW TABLES;
) tables

UNION ALL

SELECT 
    'Sample Weather Data',
    'hive_catalog',
    'raw_data',
    'weather',
    CONCAT('Temp: ', CAST(ROUND(temperature, 1) AS STRING), '°C') as storage_technology,
    CONCAT('Price: $', CAST(ROUND(energy_price, 2) AS STRING)) as location
FROM raw_data.weather
LIMIT 3;

-- ============================================================================
-- 4. PostgreSQL via JDBC (postgres_catalog)
-- ============================================================================

SET CATALOG postgres_catalog;

SELECT 
    'PostgreSQL Storage' as info_type,
    'postgres_catalog' as catalog,
    'public' as database_name,
    'DBS, TBLS' as table_name,
    'PostgreSQL RDBMS' as storage_technology,
    'Hive Metastore Backend' as location
FROM dual

UNION ALL

SELECT 
    'Metastore Stats',
    'postgres_catalog',
    'public',
    'Total Databases',
    CAST(COUNT(*) AS STRING) as storage_technology,
    'In Hive Metastore' as location
FROM public.DBS

UNION ALL

SELECT 
    'Metastore Stats',
    'postgres_catalog',
    'public',
    'Total Tables',
    CAST(COUNT(*) AS STRING) as storage_technology,
    'In Hive Metastore' as location
FROM public.TBLS;

-- ============================================================================
-- 5. Cross-Catalog Summary - The Power of Multi-Connector!
-- ============================================================================

SET CATALOG default_catalog;

SELECT 
    '=== MULTI-CONNECTOR SUMMARY ===' as summary_type,
    'Technology' as detail,
    'Location' as storage_info,
    'Sample Query' as query_example
FROM dual

UNION ALL

SELECT 
    'StarRocks Native OLAP',
    'default_catalog.energy_trading.trading_decisions',
    'Local high-performance columnar storage',
    'SELECT * FROM default_catalog.energy_trading.trading_decisions'

UNION ALL

SELECT 
    'Delta Lake (Azure)',
    'hive_catalog.raw_data.weather',
    'Azure Blob Storage via Hive Metastore',
    'SELECT * FROM hive_catalog.raw_data.weather'

UNION ALL

SELECT 
    'PostgreSQL (JDBC)',
    'postgres_catalog.public.DBS/TBLS',
    'Hive Metastore metadata tables',
    'SELECT * FROM postgres_catalog.public.DBS'

UNION ALL

SELECT 
    '🎉 ALL ACCESSIBLE',
    'Through Single MySQL Endpoint',
    'localhost:9030',
    'mysql -h localhost -P 9030 -u root';

-- ============================================================================
-- 6. The Ultimate Multi-Connector Query
-- Join data from ALL THREE storage technologies in ONE query!
-- ============================================================================

SELECT 
    'ULTIMATE CROSS-STORAGE JOIN' as demo_name,
    COUNT(DISTINCT td.id) as trading_decisions,
    ROUND(AVG(w.temperature), 2) as avg_temperature,
    ROUND(AVG(w.energy_price), 2) as avg_energy_price,
    (SELECT COUNT(*) FROM postgres_catalog.public.DBS) as hive_databases,
    (SELECT COUNT(*) FROM postgres_catalog.public.TBLS) as hive_tables,
    'StarRocks + Delta + Postgres' as storage_technologies
FROM default_catalog.energy_trading.trading_decisions td
CROSS JOIN hive_catalog.raw_data.weather w
LIMIT 1;
