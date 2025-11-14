#!/bin/bash
# Stage 5: Multi-Catalog Analytics - The Grand Finale
# This demonstrates StarRocks' ultimate power: querying 3 different storage systems in ONE query

echo "============================================================================"
echo "STAGE 5: Multi-Catalog Analytics - The Grand Finale 🎯"
echo "============================================================================"
echo ""
echo "What happens in this stage:"
echo "  • Create a StarRocks native table that JOINS data from ALL 3 catalogs"
echo "  • Single SQL query combining:"
echo "    1. default_catalog → StarRocks native OLAP (trading_decisions)"
echo "    2. hive_catalog → Delta Lake on Azure (weather data)"
echo "    3. postgres_catalog → PostgreSQL JDBC (Hive metadata)"
echo ""
echo "The All Query:"
echo "  INSERT INTO decision_weather_analytics"
echo "  SELECT"
echo "    td.id, td.predicted_price, td.decision,  -- from StarRocks OLAP"
echo "    AVG(w.temperature), AVG(w.humidity),     -- from Delta Lake"
echo "    COUNT(*) FROM postgres_catalog.DBS       -- from PostgreSQL"
echo "  FROM default_catalog.trading_decisions td"
echo "  CROSS JOIN hive_catalog.raw_data.weather w"
echo ""
echo "no ETL, no data movement, just query!"
echo ""
echo "----------------------------------------------------------------------------"
echo "📋 ACTION: Opening Dagster UI for multi_catalog_analytics asset..."
echo "----------------------------------------------------------------------------"

# Open Dagster UI to the multi_catalog_analytics asset
open "http://localhost:3000/assets/multi_catalog_analytics"

read -p "Press Enter once materialization is complete..."

echo ""
echo "🔍 Verifying the multi-catalog analytics table..."
echo ""

# Query the final analytics table
echo ""
echo "📝 MySQL Query:"
echo "USE energy_trading; SELECT * FROM decision_weather_analytics LIMIT 3;"
echo ""
echo "The unified analytics table (data from 3 storage technologies):"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e \
    "USE energy_trading; SELECT * FROM decision_weather_analytics LIMIT 3;"

echo ""
read -p "Press Enter to check row count..."

echo ""
echo "📝 MySQL Query:"
echo "SELECT COUNT(*) as total_analytics_rows FROM energy_trading.decision_weather_analytics;"
echo ""
echo "Row count in analytics table:"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e \
    "SELECT COUNT(*) as total_analytics_rows FROM energy_trading.decision_weather_analytics;"

echo ""
echo "============================================================================"
echo "✅ STAGE 5 COMPLETE - Multi-Catalog Analytics Created!"
echo "============================================================================"
echo ""
echo "What we now have:"
echo "  • Table: energy_trading.decision_weather_analytics"
echo "  • Data from 3 different storage technologies in ONE table:"
echo "    ✓ StarRocks native OLAP (trading decisions)"
echo "    ✓ Delta Lake on Azure (weather data)"
echo "    ✓ PostgreSQL via JDBC (metadata counts)"
echo ""
echo "============================================================================"
echo "🎉 ALL STAGES COMPLETE!"
echo "============================================================================"
echo ""

echo ""
echo "============================================================================"
echo "� DEMO COMPLETE - Full Pipeline with Multi-Catalog Query!"
echo "============================================================================"
echo "Architecture:"
echo "  ┌─────────────────────────────────────────┐"
echo "  │      StarRocks Multi-Catalog Engine     │"
echo "  │      (MySQL Protocol: localhost:9030)   │"
echo "  └─────────────────────────────────────────┘"
echo "           │              │              │"
echo "           ▼              ▼              ▼"
echo "     Native OLAP    Delta Lake      PostgreSQL"
echo "     (Columnar)     (Azure Blob)    (JDBC)"
echo ""
echo "============================================================================"
read -p "Press Enter to finish..."
