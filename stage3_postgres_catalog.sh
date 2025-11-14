#!/bin/bash
# Stage 3: Create PostgreSQL External Catalog in StarRocks
# This connects StarRocks directly to PostgreSQL (Hive Metastore backend database)

echo "============================================================================"
echo "STAGE 3: Create PostgreSQL External Catalog in StarRocks"
echo "============================================================================"
echo ""
echo "What happens in this stage:"
echo "  • Create 'postgres_catalog' external catalog in StarRocks"
echo "  • Connect via JDBC to PostgreSQL database"
echo "  • Access Hive Metastore backend tables (DBS, TBLS, COLUMNS_V2, etc.)"
echo "  • Query metadata directly from PostgreSQL"
echo ""
echo "Key concept: Same PostgreSQL database, different access method"
echo "  • Stage 2: StarRocks → Hive Metastore Thrift API → PostgreSQL"
echo "  • Stage 3: StarRocks → JDBC → PostgreSQL (direct)"
echo ""
echo "----------------------------------------------------------------------------"
echo "📋 ACTION: Opening Dagster UI for postgres_external_catalog asset..."
echo "----------------------------------------------------------------------------"

# Open Dagster UI to the postgres_external_catalog asset
open "http://localhost:3000/assets/postgres_external_catalog"

read -p "Press Enter once materialization is complete..."

echo ""
echo "🔍 Verifying the PostgreSQL catalog was created..."
echo ""
read -p "Press Enter to list all catalogs (should see 3 now)..."

# Show all catalogs in StarRocks (should now have 3)
echo ""
echo "📝 MySQL Query:"
echo "SHOW CATALOGS;"
echo ""
echo "Available catalogs in StarRocks:"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e "SHOW CATALOGS;"

echo ""
read -p "Press Enter to show databases in postgres_catalog..."

echo ""
echo "📝 MySQL Query:"
echo "SET CATALOG postgres_catalog; SHOW DATABASES;"
echo ""
echo "Databases in postgres_catalog:"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e "SET CATALOG postgres_catalog; SHOW DATABASES;"

echo ""
read -p "Press Enter to query Hive Metastore metadata via PostgreSQL JDBC..."

echo ""
echo "📝 MySQL Query:"
echo "SET CATALOG postgres_catalog; SELECT COUNT(*) as hive_databases FROM public.DBS;"
echo ""
echo "Hive Metastore metadata queried via PostgreSQL JDBC:"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e "SET CATALOG postgres_catalog; SELECT COUNT(*) as hive_databases FROM public.DBS;"

echo ""
echo "📝 MySQL Query:"
echo "SET CATALOG postgres_catalog; SELECT COUNT(*) as hive_tables FROM public.TBLS;"
echo ""
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e "SET CATALOG postgres_catalog; SELECT COUNT(*) as hive_tables FROM public.TBLS;"

echo ""
echo "============================================================================"
echo "✅ STAGE 3 COMPLETE"
echo "============================================================================"
echo ""
echo "What we now have:"
echo "  • StarRocks can query PostgreSQL via 'postgres_catalog'"
echo "  • Catalog type: JDBC (external)"
echo "  • Data source: PostgreSQL database"
echo "  • Tables accessible: DBS, TBLS, COLUMNS_V2, PARTITIONS, etc."
echo ""
echo "Summary so far:"
echo "  ✓ default_catalog: StarRocks native OLAP storage"
echo "  ✓ hive_catalog: Delta Lake on Azure Blob Storage"
echo "  ✓ postgres_catalog: PostgreSQL via JDBC"
echo ""
echo "Next: Run ./stage4_ai_pipeline.sh to add ML model and AI trading decisions"
echo "============================================================================"
read -p "Press Enter to go to next stage..."
