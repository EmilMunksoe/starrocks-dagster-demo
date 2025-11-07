#!/bin/bash

# Multi-Catalog Demo Script
# This demonstrates StarRocks' ability to query multiple data sources through a single MySQL endpoint

echo "=================================================="
echo "StarRocks Multi-Catalog Demonstration"
echo "=================================================="
echo ""

# Wait for StarRocks to be ready
echo "Waiting for StarRocks to be ready..."
sleep 10

echo "1. Showing all available catalogs:"
echo "   - default_catalog: StarRocks native OLAP storage"
echo "   - hive_catalog: Delta Lake tables via Hive Metastore"
echo "   - postgres_catalog: PostgreSQL Hive Metastore backend"
echo ""
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW CATALOGS;"
echo ""

echo "2. Querying StarRocks native catalog (default_catalog):"
echo "   Database: energy_trading"
echo "   Table: trading_decisions (OLAP table)"
echo ""
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "
SET CATALOG default_catalog;
USE energy_trading;
SHOW TABLES;
SELECT COUNT(*) as total_decisions FROM trading_decisions;
SELECT * FROM trading_decisions ORDER BY timestamp DESC LIMIT 3;
"
echo ""

echo "3. Querying Hive catalog (hive_catalog) - Delta Lake tables:"
echo "   Database: raw_data"
echo "   Table: weather (Delta Lake on Azure Storage)"
echo ""
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "
SET CATALOG hive_catalog;
SHOW DATABASES;
USE raw_data;
SHOW TABLES;
SELECT COUNT(*) as total_weather_records FROM weather;
SELECT * FROM weather LIMIT 3;
"
echo ""

echo "4. Querying PostgreSQL catalog (postgres_catalog):"
echo "   Database: metastore"
echo "   Tables: Hive Metastore backend tables"
echo ""
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "
SET CATALOG postgres_catalog;
SHOW DATABASES;
USE metastore;
SHOW TABLES;
SELECT COUNT(*) as total_databases FROM DBS;
SELECT DB_ID, NAME, DB_LOCATION_URI FROM DBS;
SELECT COUNT(*) as total_tables FROM TBLS;
SELECT TBL_ID, TBL_NAME, TBL_TYPE FROM TBLS;
"
echo ""

echo "5. Multi-Catalog Analytics Table:"
echo "   This table joins data from all three catalogs!"
echo ""
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "
SET CATALOG default_catalog;
USE energy_trading;
SELECT * FROM decision_weather_analytics LIMIT 5;
"
echo ""

echo "6. Cross-Catalog JOIN Query Example:"
echo "   Joining default_catalog.energy_trading.trading_decisions"
echo "   with hive_catalog.raw_data.weather"
echo ""
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root -e "
SELECT 
    td.id,
    td.predicted_price,
    td.should_trade,
    td.model_name,
    AVG(w.temperature) as avg_temp,
    AVG(w.energy_price) as avg_energy_price
FROM default_catalog.energy_trading.trading_decisions td
CROSS JOIN hive_catalog.raw_data.weather w
GROUP BY td.id, td.predicted_price, td.should_trade, td.model_name
LIMIT 5;
"
echo ""

echo "=================================================="
echo "🎉 Multi-Catalog Demo Complete!"
echo "=================================================="
echo ""
echo "Key Takeaways:"
echo "  ✓ StarRocks provides a SINGLE MySQL endpoint (localhost:9030)"
echo "  ✓ Can query 3 different data sources:"
echo "    1. Native OLAP storage (fast analytics)"
echo "    2. Delta Lake via Hive Metastore (data lake)"
echo "    3. PostgreSQL backend (relational database)"
echo "  ✓ Seamless cross-catalog JOINs"
echo "  ✓ No ETL needed - query data where it lives!"
echo ""
echo "Connect yourself: mysql -h localhost -P 9030 -u root"
echo "=================================================="
