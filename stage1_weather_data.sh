#!/bin/bash
# Stage 1: Initialize Delta Lake with Weather Data
# This creates the foundation - writing data to Azure Blob Storage in Delta Lake format

echo "============================================================================"
echo "STAGE 1: Initialize Delta Lake with Weather Data"
echo "============================================================================"
echo ""
echo "What happens in this stage:"
echo "  • Generate sample weather data (temperature, humidity, wind speed, energy price)"
echo "  • Write data to Azure Blob Storage in Delta Lake format (Parquet files)"
echo "  • Register the table in Hive Metastore via Thrift"
echo ""
echo "Storage technologies involved:"
echo "  ✓ Azure Blob Storage (ABFSS protocol)"
echo "  ✓ Delta Lake (Parquet with transaction log)"
echo "  ✓ Apache Hive Metastore (metadata storage)"
echo ""
echo "----------------------------------------------------------------------------"
echo "📋 ACTION: Opening Dagster UI for weather_data asset..."
echo "----------------------------------------------------------------------------"

# Open Dagster UI to the weather_data asset
open "http://localhost:3000/assets/weather_data"

read -p "Press Enter once materialization is complete..."

echo ""
echo "🔍 Verifying what we created..."
echo ""
echo "📝 MySQL Query:"
echo "SHOW CATALOGS; SET CATALOG hive_catalog; SELECT * FROM raw_data.weather LIMIT 5;"
echo ""
echo "Weather data preview (from Delta Lake via Hive Metastore):"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e "SHOW CATALOGS; SET CATALOG hive_catalog; SELECT * FROM raw_data.weather LIMIT 5;" 2>/dev/null || \
    echo "⚠️  Hive catalog not yet created (coming in Stage 2)"

echo ""
echo "============================================================================"
echo "✅ STAGE 1 COMPLETE"
echo "============================================================================"
echo ""
echo "What we now have:"
echo "  • ~350 weather records stored in Azure Blob Storage (Delta Lake)"
echo "  • Delta Lake transaction log created"
echo "  • Table 'raw_data.weather' registered in Hive Metastore"
echo ""
echo "Next: Run ./stage2_hive_catalog.sh to connect StarRocks to this Delta Lake"
echo "============================================================================"
read -p "Press Enter to go to next stage..."
