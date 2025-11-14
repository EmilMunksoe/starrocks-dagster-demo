"""PostgreSQL external catalog asset - connects to Hive Metastore backend database"""
from dagster import asset, AssetExecutionContext

from ..utils import get_starrocks_connection


@asset(deps=["weather_data"])
def postgres_external_catalog(context: AssetExecutionContext) -> None:
    """Create PostgreSQL external catalog in StarRocks pointing to Hive Metastore backend"""
    catalog_name = "postgres_catalog"
    
    # Don't specify database - just connect to StarRocks
    with get_starrocks_connection() as (conn, cursor):
        # Drop catalog if exists
        try:
            context.log.info(f"Dropping existing catalog '{catalog_name}' if it exists")
            cursor.execute(f"DROP CATALOG IF EXISTS {catalog_name}")
        except Exception as e:
            context.log.info(f"Catalog drop not needed: {e}")
        
        # Create PostgreSQL external catalog
        create_catalog_sql = f"""
        CREATE EXTERNAL CATALOG {catalog_name}
        PROPERTIES (
            "type" = "jdbc",
            "user" = "hive",
            "password" = "hive",
            "jdbc_uri" = "jdbc:postgresql://hive-postgres:5432/metastore",
            "driver_url" = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar",
            "driver_class" = "org.postgresql.Driver"
        )
        """
        
        context.log.info(f"Creating PostgreSQL external catalog '{catalog_name}'")
        cursor.execute(create_catalog_sql)
        context.log.info(f"✅ Successfully created PostgreSQL catalog '{catalog_name}'")
        
        # Verify catalog creation
        cursor.execute("SHOW CATALOGS")
        catalogs = cursor.fetchall()
        context.log.info(f"Available catalogs: {[cat[0] for cat in catalogs]}")
        
        # Show databases in PostgreSQL catalog
        cursor.execute(f"SET CATALOG {catalog_name}")
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        context.log.info(f"Databases in {catalog_name}: {[db[0] for db in databases]}")
        
        # Show tables in public schema (where Hive Metastore tables are)
        cursor.execute("USE public")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        context.log.info(f"Tables in public schema: {len(tables)} tables found")
        
        # Query Hive metadata - use simple queries without joins or quotes
        cursor.execute('SELECT COUNT(*) as db_count FROM DBS')
        db_count = cursor.fetchone()[0]
        context.log.info(f"Total Hive databases in metastore: {db_count}")
        
        cursor.execute('SELECT COUNT(*) as table_count FROM TBLS')
        table_count = cursor.fetchone()[0]
        context.log.info(f"Total Hive tables in metastore: {table_count}")
        
        # Sample a few database names (limit to avoid issues)
        cursor.execute('SELECT * FROM DBS LIMIT 3')
        sample_dbs = cursor.fetchall()
        context.log.info(f"Sample databases: {len(sample_dbs)} records retrieved")
        
        context.log.info("🎉 PostgreSQL catalog is ready for multi-connector queries!")
