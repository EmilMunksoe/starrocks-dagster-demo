"""Weather data asset - generates data and writes to Delta Lake with Hive Metastore registration"""
import pandas as pd
import numpy as np
from dagster import asset, AssetExecutionContext, Config
from pydantic import Field
from deltalake import write_deltalake

from ..config import (
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_STORAGE_ACCOUNT_KEY,
    AZURE_STORAGE_CONTAINER,
    HIVE_METASTORE_HOST,
    HIVE_METASTORE_PORT
)


class WeatherDataConfig(Config):
    """Configuration for weather data generation"""
    num_samples: int = Field(
        default=25,
        description="Number of sample weather records to generate"
    )


@asset
def weather_data(context: AssetExecutionContext, config: WeatherDataConfig) -> pd.DataFrame:
    """Generate weather data and write to Delta Lake with Hive Metastore registration"""
    
    namespace = "raw_data"
    table_name = "weather"
    storage_location = f"abfss://{AZURE_STORAGE_CONTAINER}@{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/weather"
    
    df = _generate_sample_data(context, config.num_samples)
    _write_to_delta_lake(context, df, storage_location)
    _register_in_hive_metastore(context, namespace, table_name, storage_location)
    
    return df


def _generate_sample_data(context: AssetExecutionContext, num_samples: int) -> pd.DataFrame:
    """Generate random sample weather data"""
    np.random.seed()
    
    temperatures = np.random.uniform(10, 35, num_samples)
    humidities = np.random.uniform(30, 90, num_samples)
    wind_speeds = np.random.uniform(0, 20, num_samples)
    
    base_prices = np.random.uniform(30, 100, num_samples)
    temp_effect = temperatures * 0.5
    wind_effect = -wind_speeds * 1.5
    humidity_effect = np.random.normal(0, 5, num_samples)
    
    energy_prices = base_prices + temp_effect + wind_effect + humidity_effect
    energy_prices = np.clip(energy_prices, 20, 150)
    
    df = pd.DataFrame({
        'temperature': temperatures,
        'humidity': humidities,
        'wind_speed': wind_speeds,
        'energy_price': energy_prices
    })
    
    context.log.info(f"Generated {len(df)} random sample records")
    return df


def _write_to_delta_lake(context: AssetExecutionContext, df: pd.DataFrame, storage_location: str) -> None:
    """Write DataFrame to Delta Lake format on Azure Storage"""
    try:
        storage_options = {
            "AZURE_STORAGE_ACCOUNT_NAME": AZURE_STORAGE_ACCOUNT_NAME,
            "AZURE_STORAGE_ACCOUNT_KEY": AZURE_STORAGE_ACCOUNT_KEY
        }
        
        context.log.info(f"Writing {len(df)} records as Delta Lake to {storage_location}")
        write_deltalake(storage_location, df, mode="overwrite", storage_options=storage_options)
        context.log.info("Successfully wrote Delta Lake data to Azure Storage")
        
    except Exception as e:
        context.log.error(f"Failed to write Delta Lake data: {e}")
        raise


def _register_in_hive_metastore(context: AssetExecutionContext, namespace: str, table_name: str, storage_location: str) -> None:
    """Register Delta Lake table in Hive Metastore via Thrift"""
    try:
        from hmsclient import HMSClient
        from hmsclient.genthrift.hive_metastore.ttypes import (
            Database, Table, StorageDescriptor, SerDeInfo, FieldSchema
        )
        
        context.log.info(f"Connecting to Hive Metastore at {HIVE_METASTORE_HOST}:{HIVE_METASTORE_PORT}")
        
        # Connect to Hive Metastore via Thrift
        client = HMSClient(host=HIVE_METASTORE_HOST, port=HIVE_METASTORE_PORT)
        client.open()
        
        # Create database if not exists
        try:
            db = client.get_database(namespace)
            context.log.info(f"Database '{namespace}' already exists")
        except Exception:
            context.log.info(f"Creating database '{namespace}'")
            db = Database(
                name=namespace,
                description=f"Database for {namespace} tables",
                locationUri=f"abfss://{AZURE_STORAGE_CONTAINER}@{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{namespace}",
                parameters={}
            )
            client.create_database(db)
        
        # Check if table exists and drop if needed
        try:
            existing_table = client.get_table(dbname=namespace, tbl_name=table_name)
            if existing_table:
                context.log.info(f"Table {namespace}.{table_name} exists, dropping it")
                client.drop_table(dbname=namespace, tbl_name=table_name)
        except Exception:
            context.log.info(f"Table {namespace}.{table_name} does not exist yet")
        
        # Define table schema
        cols = [
            FieldSchema(name='temperature', type='double', comment='Temperature in Celsius'),
            FieldSchema(name='humidity', type='double', comment='Humidity percentage'),
            FieldSchema(name='wind_speed', type='double', comment='Wind speed in m/s'),
            FieldSchema(name='energy_price', type='double', comment='Energy price')
        ]
        
        # Define storage descriptor
        sd = StorageDescriptor(
            cols=cols,
            location=storage_location,
            inputFormat='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            outputFormat='org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            compressed=False,
            numBuckets=-1,
            serdeInfo=SerDeInfo(
                name=table_name,
                serializationLib='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                parameters={}
            ),
            bucketCols=[],
            sortCols=[],
            parameters={}
        )
        
        # Create table object
        table = Table(
            tableName=table_name,
            dbName=namespace,
            owner='dagster',
            createTime=0,
            lastAccessTime=0,
            retention=0,
            sd=sd,
            partitionKeys=[],
            parameters={'EXTERNAL': 'TRUE'},
            tableType='EXTERNAL_TABLE'
        )
        
        # Register table in Hive Metastore
        context.log.info(f"Registering table '{namespace}.{table_name}' in Hive Metastore")
        client.create_table(table)
        
        context.log.info(f"✅ Successfully registered table {namespace}.{table_name} in Hive Metastore")
        
        # Verify table
        registered_table = client.get_table(dbname=namespace, tbl_name=table_name)
        context.log.info(f"Verified table location: {registered_table.sd.location}")
        
        client.close()
        
    except ImportError:
        context.log.warning("hmsclient not installed, skipping Hive Metastore registration")
        context.log.info(f"Delta Lake table written to: {storage_location}")
        context.log.info("Table will be accessible via StarRocks Hive external catalog")
    except Exception as e:
        context.log.error(f"Failed to register table in Hive Metastore: {e}")
        context.log.info(f"Delta Lake table written to: {storage_location}")
        context.log.info("You may need to manually create the table in Hive or use StarRocks to access it")