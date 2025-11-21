#!/bin/bash
# Create StarRocks external catalogs
# Usage: ./create_catalogs.sh [hive_catalog|postgres_catalog]

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

if [ "$1" == "hive_catalog" ]; then
    mysql -h localhost -P 9030 -u root -p --protocol=TCP <<EOF
CREATE EXTERNAL CATALOG hive_catalog
PROPERTIES (
    "type" = "hive",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://hive-metastore:9083",
    "azure.adls2.storage_account" = "${AZURE_STORAGE_ACCOUNT_NAME}",
    "azure.adls2.shared_key" = "${AZURE_STORAGE_ACCOUNT_KEY}"
);
EOF
    echo "✅ Created hive_catalog"

elif [ "$1" == "postgres_catalog" ]; then
    mysql -h localhost -P 9030 -u root -p --protocol=TCP <<'EOF'
CREATE EXTERNAL CATALOG postgres_catalog
PROPERTIES (
    "type" = "jdbc",
    "user" = "hive",
    "password" = "hive",
    "jdbc_uri" = "jdbc:postgresql://hive-postgres:5432/metastore",
    "driver_url" = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
    "driver_class" = "org.postgresql.Driver"
);
EOF
    echo "✅ Created postgres_catalog"

else
    echo "Usage: $0 [hive_catalog|postgres_catalog]"
    exit 1
fi
