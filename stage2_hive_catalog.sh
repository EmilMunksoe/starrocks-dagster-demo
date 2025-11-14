#!/bin/bash
# Stage 2: Create Hive External Catalog in StarRocks
# This connects StarRocks to the Delta Lake via Hive Metastore

echo "============================================================================"
echo "STAGE 2: Create Hive External Catalog in StarRocks"
echo "============================================================================"
echo ""
echo "What happens in this stage:"
echo "  • Create 'hive_catalog' external catalog in StarRocks"
echo "  • Connect to Hive Metastore via Thrift (thrift://hive-metastore:9083)"
echo "  • Configure Azure Storage credentials for data access"
echo "  • Query Delta Lake data through StarRocks"
echo ""
echo "Key concept: StarRocks reads metadata from Hive Metastore, data from Azure"
echo ""
echo "----------------------------------------------------------------------------"
echo "📋 ACTION: Opening Dagster UI for delta_external_catalog asset..."
echo "----------------------------------------------------------------------------"

# Open Dagster UI to the delta_external_catalog asset
open "http://localhost:3000/assets/delta_external_catalog"

read -p "Press Enter once materialization is complete..."

echo ""
echo "🔍 Verifying the Hive catalog was created..."
echo ""

# Show all catalogs in StarRocks
echo ""
echo "📝 MySQL Query:"
echo "SHOW CATALOGS;"
echo ""
echo "Available catalogs in StarRocks:"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e "SHOW CATALOGS;"

echo ""
read -p "Press Enter to show databases in hive_catalog..."

echo ""
echo "📝 MySQL Query:"
echo "SET CATALOG hive_catalog; SHOW DATABASES;"
echo ""
echo "Databases in hive_catalog:"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e "SET CATALOG hive_catalog; SHOW DATABASES;"

echo ""
read -p "Press Enter to query weather data through hive_catalog..."

echo ""
echo "📝 MySQL Query:"
echo "SET CATALOG hive_catalog; SELECT COUNT(*) as total_weather_records FROM raw_data.weather;"
echo ""
echo "Weather data queried through hive_catalog (Delta Lake → StarRocks):"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e "SET CATALOG hive_catalog; SELECT COUNT(*) as total_weather_records FROM raw_data.weather;"

echo ""
echo "============================================================================"
echo "✅ STAGE 2 COMPLETE"
echo "============================================================================"
echo ""
echo "What we now have:"
echo "  • StarRocks can query Delta Lake via 'hive_catalog'"
echo "  • Catalog type: Hive (external)"
echo "  • Data source: Azure Blob Storage"
echo "  • Metadata source: Hive Metastore"
echo ""
echo "Next: Run ./stage3_postgres_catalog.sh to add PostgreSQL connector"
echo "============================================================================"
read -p "Press Enter to go to next stage..."
