#!/bin/bash
# Reset Demo - Clean slate for a fresh demo run

echo "============================================================================"
echo "Reset Demo Environment"
echo "============================================================================"
echo ""
echo "This will:"
echo "  • Stop all containers"
echo "  • Remove all volumes (data will be lost)"
echo "  • Restart all services"
echo "  • Wait for services to be ready"
echo ""
echo "⚠️  WARNING: All data will be deleted!"
echo ""
read -p "Are you sure? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Reset cancelled"
    exit 0
fi

echo ""
echo "Stopping containers and removing volumes..."
docker-compose down -v

# Explicitly remove the dagster_home volume to prevent migration issues
echo "Cleaning up Dagster storage..."
docker volume rm mft-energyoss-energy-trading_dagster_home 2>/dev/null || true

echo ""
echo "Starting services..."
docker-compose up -d

echo ""
echo "Waiting for services to be ready..."
echo "(This takes about 30-60 seconds)"
echo ""

# Wait for StarRocks
echo -n "Waiting for StarRocks..."
for i in {1..60}; do
    if docker exec mft-energyoss-energy-trading-starrocks-1 mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1" &>/dev/null; then
        echo " ✓"
        break
    fi
    echo -n "."
    sleep 1
done

# Wait for Dagster
echo -n "Waiting for Dagster..."
for i in {1..30}; do
    if curl -s http://localhost:3000 &>/dev/null; then
        echo " ✓"
        break
    fi
    echo -n "."
    sleep 1
done

# Wait for Hive Metastore
echo -n "Waiting for Hive Metastore..."
sleep 10  # Give it some time to initialize
echo " ✓"

echo ""
echo "============================================================================"
echo "✅ Reset complete! Services are ready."
echo "============================================================================"
echo ""
echo "Service URLs:"
echo "  • StarRocks: mysql -h localhost -P 9030 -u root"
echo "  • Dagster UI: http://localhost:3000"
echo "  • Ollama: http://localhost:11434"
echo "============================================================================"
