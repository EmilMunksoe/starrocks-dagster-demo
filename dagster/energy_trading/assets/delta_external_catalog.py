"""Hive external catalog asset - demonstrates StarRocks querying Delta Lake via Hive Metastore"""

import pandas as pd
from dagster import asset, AssetExecutionContext

from ..config import (
    HIVE_METASTORE_HOST,
    HIVE_METASTORE_PORT,
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_STORAGE_ACCOUNT_KEY,
)
from ..utils import get_starrocks_connection


@asset(deps=["weather_data"])
def delta_external_catalog(context: AssetExecutionContext) -> pd.DataFrame:
    """Create Hive external catalog in StarRocks and query Delta Lake via Hive Metastore"""
    catalog_name = "hive_catalog"

    # Don't specify database - just connect to StarRocks
    with get_starrocks_connection() as (conn, cursor):
        context.log.info(
            f"🔧 Creating Hive external catalog '{catalog_name}' in StarRocks..."
        )

        # Drop existing catalog if it exists
        try:
            cursor.execute(f"DROP CATALOG IF EXISTS {catalog_name};")
        except Exception as e:
            context.log.warning(f"Could not drop existing catalog: {e}")

        # Create Hive external catalog pointing to Hive Metastore
        create_catalog_sql = f"""
        CREATE EXTERNAL CATALOG {catalog_name}
        PROPERTIES (
            "type" = "hive",
            "hive.metastore.type" = "hive",
            "hive.metastore.uris" = "thrift://{HIVE_METASTORE_HOST}:{HIVE_METASTORE_PORT}",
            "azure.adls2.storage_account" = "{AZURE_STORAGE_ACCOUNT_NAME}",
            "azure.adls2.shared_key" = "{AZURE_STORAGE_ACCOUNT_KEY}"
        )
        """

        try:
            cursor.execute(create_catalog_sql)
            context.log.info(f"✅ Created Hive external catalog '{catalog_name}'")
        except Exception as e:
            context.log.error(f"❌ Failed to create Hive external catalog: {e}")
            return pd.DataFrame(columns=["status"])

        # Show catalogs to verify creation
        cursor.execute("SHOW CATALOGS;")
        catalogs = cursor.fetchall()
        catalog_names = [cat[0] for cat in catalogs]
        context.log.info(f"📚 Available catalogs: {catalog_names}")

        # Switch to the Hive catalog
        context.log.info(f"🔄 Switching to catalog: {catalog_name}")
        cursor.execute(f"SET CATALOG {catalog_name};")

        # Show databases
        cursor.execute("SHOW DATABASES;")
        databases = cursor.fetchall()
        context.log.info(
            f"📂 Databases in {catalog_name}: {[db[0] for db in databases]}"
        )

        # Query weather table from Delta Lake
        try:
            query = """
            SELECT
                timestamp,
                temperature,
                humidity,
                wind_speed,
                energy_price
            FROM raw_data.weather
            LIMIT 10
            """
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)

            context.log.info(
                f"✅ Successfully queried {len(df)} records from Delta Lake via Hive!"
            )
            context.log.info(f"📊 Weather data sample: {df.head(3).to_dict('records')}")
            return df

        except Exception as e:
            context.log.warning(f"⚠️ Could not query weather table: {e}")

            # Return catalog info instead
            cursor.execute("SHOW CATALOGS;")
            catalogs = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(catalogs, columns=columns)
