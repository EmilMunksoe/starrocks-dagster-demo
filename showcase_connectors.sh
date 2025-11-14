#!/bin/bash
# Multi-Connector Showcase Script
# Demonstrates different storage technologies accessible through StarRocks

echo "============================================================================"
echo "MULTI-CONNECTOR SHOWCASE"
echo "Querying 3 Different Storage Technologies Through Single MySQL Endpoint"
echo "============================================================================"
echo ""

echo "1️⃣  STARROCKS NATIVE OLAP STORAGE"
echo "   Catalog: default_catalog"
echo "   Technology: Columnar OLAP"
echo "   Location: Local StarRocks storage"
echo ""
docker exec mft-energyoss-energy-trading-starrocks-1 mysql -h 127.0.0.1 -P 9030 -u root --table -e "
SET CATALOG default_catalog;
USE energy_trading;
SELECT
    'trading_decisions' as table_name,
    'StarRocks OLAP' as storage_tech,
    'Local Storage' as location,
    COUNT(*) as row_count
FROM trading_decisions;
"
echo ""

echo "2️⃣  DELTA LAKE ON AZURE STORAGE"
echo "   Catalog: hive_catalog"
echo "   Technology: Delta Lake (Parquet files)"
echo "   Location: Azure Blob Storage (ABFSS)"
echo ""
docker exec mft-energyoss-energy-trading-starrocks-1 mysql -h 127.0.0.1 -P 9030 -u root --table -e "
SET CATALOG hive_catalog;
SELECT
    'weather' as table_name,
    'Delta Lake' as storage_tech,
    'Azure Blob (ABFSS)' as location,
    COUNT(*) as row_count
FROM raw_data.weather;
"
echo ""

echo "3️⃣  POSTGRESQL VIA JDBC"
echo "   Catalog: postgres_catalog"
echo "   Technology: PostgreSQL RDBMS"
echo "   Location: Hive Metastore Backend DB"
echo ""
docker exec mft-energyoss-energy-trading-starrocks-1 mysql -h 127.0.0.1 -P 9030 -u root --table -e "
SET CATALOG postgres_catalog;
USE public;
SELECT
    'DBS' as table_name,
    'PostgreSQL' as storage_tech,
    'JDBC Connection' as location,
    COUNT(*) as row_count
FROM DBS
UNION ALL
SELECT
    'TBLS' as table_name,
    'PostgreSQL' as storage_tech,
    'JDBC Connection' as location,
    COUNT(*) as row_count
FROM TBLS;
"
echo ""

echo "============================================================================"
echo "4️⃣  THE ULTIMATE CROSS-STORAGE QUERY"
echo "   Joining ALL THREE storage technologies in ONE query!"
echo "============================================================================"
echo ""
docker exec mft-energyoss-energy-trading-starrocks-1 mysql -h 127.0.0.1 -P 9030 -u root --table -e "
SELECT
    '🎯 Multi-Storage Query' as query_type,
    COUNT(DISTINCT td.id) as starrocks_decisions,
    COUNT(DISTINCT w.temperature) as deltalake_weather_readings,
    (SELECT COUNT(*) FROM postgres_catalog.public.DBS) as postgres_hive_dbs,
    (SELECT COUNT(*) FROM postgres_catalog.public.TBLS) as postgres_hive_tables
FROM default_catalog.energy_trading.trading_decisions td
CROSS JOIN hive_catalog.raw_data.weather w;
"
echo ""

echo "============================================================================"
echo "5️⃣  DETAILED CONNECTOR INFORMATION"
echo "============================================================================"
echo ""
docker exec mft-energyoss-energy-trading-starrocks-1 mysql -h 127.0.0.1 -P 9030 -u root --table -e "
SELECT
    'default_catalog' as catalog_name,
    'Internal' as connector_type,
    'Local columnar OLAP storage' as storage_technology,
    'High-performance analytics' as use_case
UNION ALL
SELECT
    'hive_catalog' as catalog_name,
    'Hive' as connector_type,
    'Delta Lake on Azure Blob Storage via Hive Metastore' as storage_technology,
    'Data lake queries without ETL' as use_case
UNION ALL
SELECT
    'postgres_catalog' as catalog_name,
    'Jdbc' as connector_type,
    'PostgreSQL database via JDBC connector' as storage_technology,
    'Metadata and relational data access' as use_case;
"
echo ""

echo "============================================================================"
echo "✅ DEMONSTRATION COMPLETE"
echo ""
echo "Key Takeaway: ONE MySQL endpoint (localhost:9030) queries THREE different"
echo "storage systems without any ETL or data movement!"
echo ""
echo "Connect yourself: mysql -h localhost -P 9030 -u root"
echo "============================================================================"
