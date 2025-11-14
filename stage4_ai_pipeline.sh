#!/bin/bash
# Stage 4: AI/ML Pipeline - Train Model and Generate Trading Decisions
# This adds the "fun" AI layer with ML training and Ollama-powered decisions

echo "============================================================================"
echo "STAGE 4: AI/ML Pipeline - Model Training & Trading Decisions"
echo "============================================================================"
echo ""
echo "What happens in this stage:"
echo ""
echo "Part 1 - Train ML Model:"
echo "  • Query weather data from Delta Lake via Hive catalog"
echo "  • Train LinearRegression model"
echo ""
echo "Part 2 - AI Trading Decisions:"
echo "  • Use trained model to predict next energy price"
echo "  • Send prediction to Ollama AI (local LLM)"
echo "  • AI decides: should we trade or not?"
echo "  • Store decision in StarRocks native table"
echo "  • Also write to Delta Lake and register in Hive Metastore"
echo ""
echo "Technologies showcased:"
echo "  • scikit-learn for ML training"
echo "  • Ollama for AI decision-making"
echo "  • StarRocks native OLAP storage"
echo "  • Delta Lake + Hive Metastore registration"
echo ""
echo "----------------------------------------------------------------------------"
echo "📋 ACTION 1: Opening Dagster UI for trained_model asset..."
echo "----------------------------------------------------------------------------"

# Open Dagster UI to the trained_model asset
open "http://localhost:3000/assets/trained_model"

read -p "Press Enter once trained_model materialization is complete..."

echo ""
echo "✅ Model trained successfully!"
echo ""
echo "----------------------------------------------------------------------------"
echo "📋 ACTION 2: Opening Dagster UI for trading_decision asset..."
echo "----------------------------------------------------------------------------"

# Open Dagster UI to the trading_decision asset
open "http://localhost:3000/assets/trading_decision"

read -p "Press Enter once trading_decision materialization is complete..."

echo ""
echo "🔍 Verifying what we created..."
echo ""
read -p "Press Enter to view trading decisions in StarRocks OLAP..."

# Show trading decisions in StarRocks native catalog
echo ""
echo "📝 MySQL Query:"
echo "USE energy_trading; SELECT * FROM trading_decisions ORDER BY timestamp DESC LIMIT 5;"
echo ""
echo "Trading decisions stored in StarRocks OLAP (default_catalog):"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e \
    "USE energy_trading; SELECT * FROM trading_decisions ORDER BY timestamp DESC LIMIT 5;"

echo ""
read -p "Press Enter to check trading decisions in Delta Lake..."

echo ""
echo "📝 MySQL Query:"
echo "SET CATALOG hive_catalog; SELECT COUNT(*) as total_decisions FROM analytics.trading_decisions;"
echo ""
echo "Trading decisions also stored in Delta Lake (hive_catalog):"
docker exec mft-energyoss-energy-trading-starrocks-1 \
    mysql -h 127.0.0.1 -P 9030 -u root --table -e \
    "SET CATALOG hive_catalog; SELECT COUNT(*) as total_decisions FROM analytics.trading_decisions;" 2>/dev/null || \
    echo "⚠️  Trading decisions not yet in Hive catalog (may take a moment to sync)"

echo ""
echo "============================================================================"
echo "✅ STAGE 4 COMPLETE"
echo "============================================================================"
echo ""
echo "What we now have:"
echo "  • Trained ML model (LinearRegression on weather → energy price)"
echo "  • AI-generated trading decisions stored in:"
echo "    ✓ StarRocks native table: default_catalog.energy_trading.trading_decisions"
echo "    ✓ Delta Lake: Azure Blob Storage (analytics.trading_decisions)"
echo "    ✓ Hive Metastore: Table registered for external access"
echo ""
echo "Next: Run ./stage5_multi_catalog.sh to combine ALL data sources in one query"
echo "============================================================================"
read -p "Press Enter to go to next stage..."
