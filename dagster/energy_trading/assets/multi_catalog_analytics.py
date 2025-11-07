"""Multi-catalog joined table - demonstrates StarRocks' ability to query across multiple data sources"""
from dagster import asset, AssetExecutionContext

from ..utils import get_starrocks_connection


@asset(deps=["trading_decision", "weather_data", "postgres_external_catalog"])
def multi_catalog_analytics(context: AssetExecutionContext) -> None:
    """Create a StarRocks native table joining data from 3 catalogs via single MySQL endpoint
    
    This showcases StarRocks' multi-connector capability:
    - default_catalog: StarRocks native tables (trading_decisions)
    - hive_catalog: Delta Lake tables via Hive Metastore (weather data)
    - postgres_catalog: Direct PostgreSQL connection (Hive metadata)
    """
    
    with get_starrocks_connection("energy_trading") as (conn, cursor):
        # Drop existing analytics table if exists
        context.log.info("Preparing analytics database...")
        cursor.execute("DROP TABLE IF EXISTS energy_trading.decision_weather_analytics")
        
        # Create a materialized table that joins data from all three catalogs
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS energy_trading.decision_weather_analytics (
            decision_id BIGINT,
            predicted_price FLOAT,
            decision BOOLEAN,
            decision_timestamp DATETIME,
            avg_temperature DOUBLE,
            avg_humidity DOUBLE,
            avg_wind_speed DOUBLE,
            avg_energy_price DOUBLE,
            weather_sample_count BIGINT,
            hive_db_count INT,
            hive_table_count INT
        ) ENGINE=OLAP
        DUPLICATE KEY(decision_id)
        DISTRIBUTED BY HASH(decision_id) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1"
        )
        """
        
        context.log.info("Creating multi-catalog analytics table...")
        cursor.execute(create_table_sql)
        context.log.info("✅ Table created successfully")
        
        # Insert data by joining across all three catalogs
        # Use simple COUNT queries to avoid StarRocks JDBC catalog limitations
        insert_sql = """
        INSERT INTO energy_trading.decision_weather_analytics
        WITH weather_stats AS (
            SELECT 
                AVG(temperature) as avg_temp,
                AVG(humidity) as avg_hum,
                AVG(wind_speed) as avg_wind,
                AVG(energy_price) as avg_price,
                COUNT(*) as sample_count
            FROM hive_catalog.raw_data.weather
        ),
        hive_metadata AS (
            SELECT 
                (SELECT COUNT(*) FROM postgres_catalog.public.DBS) as db_count,
                (SELECT COUNT(*) FROM postgres_catalog.public.TBLS) as table_count
        )
        SELECT 
            td.id as decision_id,
            td.predicted_price,
            td.decision,
            td.timestamp as decision_timestamp,
            ws.avg_temp,
            ws.avg_hum,
            ws.avg_wind,
            ws.avg_price,
            ws.sample_count,
            hm.db_count,
            hm.table_count
        FROM default_catalog.energy_trading.trading_decisions td
        CROSS JOIN weather_stats ws
        CROSS JOIN hive_metadata hm
        ORDER BY td.timestamp DESC
        LIMIT 100
        """
        
        context.log.info("Inserting data from multiple catalogs...")
        context.log.info("  - default_catalog: trading_decisions (StarRocks native)")
        context.log.info("  - hive_catalog: weather data (Delta Lake)")
        context.log.info("  - postgres_catalog: Hive metadata (PostgreSQL)")
        
        cursor.execute(insert_sql)
        rows_inserted = cursor.rowcount
        context.log.info(f"✅ Inserted {rows_inserted} rows from multi-catalog join")
        
        # Verify the data
        cursor.execute("SELECT COUNT(*) FROM energy_trading.decision_weather_analytics")
        count = cursor.fetchone()[0]
        context.log.info(f"Total rows in analytics table: {count}")
        
        # Show sample data
        cursor.execute("""
            SELECT 
                decision_id,
                predicted_price,
                decision,
                avg_temperature,
                avg_energy_price,
                weather_sample_count,
                hive_db_count,
                hive_table_count
            FROM energy_trading.decision_weather_analytics 
            LIMIT 5
        """)
        
        context.log.info("Sample data from multi-catalog analytics:")
        for row in cursor.fetchall():
            context.log.info(f"  Decision {row[0]}: price=${row[1]:.2f}, trade={row[2]}, "
                           f"temp={row[3]:.1f}°C, energy=${row[4]:.2f}, "
                           f"samples={row[5]}, hive_dbs={row[6]}, hive_tables={row[7]}")
        
        # Show the complete query capabilities
        context.log.info("\n" + "="*80)
        context.log.info("🎉 MULTI-CATALOG QUERY DEMONSTRATION COMPLETE!")
        context.log.info("="*80)
        context.log.info("StarRocks can now query through a SINGLE MySQL endpoint:")
        context.log.info("  1. default_catalog.energy_trading.trading_decisions (Native OLAP)")
        context.log.info("  2. hive_catalog.raw_data.weather (Delta Lake via Hive)")
        context.log.info("  3. postgres_catalog.public.DBS/TBLS (PostgreSQL Hive Metastore)")
        context.log.info("\nAll accessible via: mysql -h localhost -P 9030 -u root")
        context.log.info("="*80 + "\n")
