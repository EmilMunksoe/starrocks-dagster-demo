"""Trading decision asset - uses Ollama AI to make trading decisions"""
from datetime import datetime, timedelta
from typing import Dict, Any

import numpy as np
import pandas as pd
import pymysql
import requests
from dagster import asset, AssetExecutionContext
from sklearn.linear_model import LinearRegression
from deltalake import write_deltalake

from ..config import (
    OLLAMA_HOST,
    OLLAMA_PORT,
    HIVE_METASTORE_HOST,
    HIVE_METASTORE_PORT,
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_STORAGE_ACCOUNT_KEY,
    AZURE_STORAGE_CONTAINER
)
from ..utils import get_starrocks_connection


@asset
def trading_decision(
    context: AssetExecutionContext, 
    weather_data: pd.DataFrame, 
    trained_model: LinearRegression
) -> Dict[str, Any]:
    """Run model on latest data and use Ollama to decide trading"""
    # Get latest weather data or generate test data
    latest_data = _get_latest_weather_data(context, weather_data)
    
    # Predict energy price
    predicted_price = trained_model.predict(latest_data)[0]
    context.log.info(f"Predicted energy price: ${predicted_price:.2f}/MWh")
    
    # Use Ollama AI to make trading decision
    should_trade = _get_trading_decision_from_ollama(context, latest_data, predicted_price)
    
    # Store decision in Delta Lake (for Unity Catalog governance)
    _store_decision_in_delta_lake(context, predicted_price, should_trade)
    
    # Also store in StarRocks (for fast querying)
    _store_decision_in_starrocks(context, predicted_price, should_trade)
    
    return {"predicted_price": predicted_price, "should_trade": should_trade}


def _get_latest_weather_data(context: AssetExecutionContext, weather_data: pd.DataFrame) -> pd.DataFrame:
    """Get the latest weather data or generate test data"""
    if len(weather_data) > 1:
        # Use latest real data if available
        latest_data = weather_data.iloc[-1:][['temperature', 'humidity', 'wind_speed']]
        context.log.info("Using latest weather data for prediction")
    else:
        # Generate test data different from training data for demo
        test_temp = np.random.uniform(15, 30)
        test_humidity = np.random.uniform(40, 80)
        test_wind = np.random.uniform(1, 15)
        latest_data = pd.DataFrame({
            'temperature': [test_temp],
            'humidity': [test_humidity], 
            'wind_speed': [test_wind]
        })
        context.log.info(
            f"Using generated test data: temp={test_temp:.1f}°C, "
            f"humidity={test_humidity:.1f}%, wind={test_wind:.1f}m/s"
        )
    
    return latest_data


def _get_trading_decision_from_ollama(
    context: AssetExecutionContext,
    latest_data: pd.DataFrame,
    predicted_price: float
) -> bool:
    """Use Ollama AI to decide whether to trade"""
    current_weather = latest_data.iloc[0]
    temp = current_weather['temperature']
    humidity = current_weather['humidity'] 
    wind_speed = current_weather['wind_speed']
    
    # Generate forecast
    current_time = datetime.now()
    forecast_temp = temp + np.random.normal(0, 2)
    forecast_humidity = humidity + np.random.normal(0, 5)
    forecast_wind = wind_speed + np.random.normal(0, 2)
    forecast_time = current_time + timedelta(hours=6)
    
    # Build prompt
    market_baseline = 55.0
    price_diff = predicted_price - market_baseline
    
    prompt = f"""You are an AI energy trading analyst. Analyze the following data and decide whether to execute a trade:

CURRENT CONDITIONS ({current_time.strftime('%Y-%m-%d %H:%M')}):
- Temperature: {temp:.1f}°C
- Humidity: {humidity:.1f}%
- Wind Speed: {wind_speed:.1f} m/s
- ML Predicted Price: ${predicted_price:.2f}/MWh (vs market baseline ${market_baseline:.2f}/MWh = {price_diff:+.2f} difference)

6-HOUR FORECAST ({forecast_time.strftime('%Y-%m-%d %H:%M')}):
- Temperature: {forecast_temp:.1f}°C  
- Humidity: {forecast_humidity:.1f}%
- Wind Speed: {forecast_wind:.1f} m/s

ENERGY MARKET ANALYSIS:
- High temperatures often increase AC usage → higher energy demand → higher prices
- High wind speeds can provide renewable energy → lower prices
- High humidity may correlate with weather patterns affecting energy consumption
- Price significantly above/below baseline indicates strong trading signal

TRADING DECISION: Based on current weather conditions, price prediction, and forecast, should we execute an energy trade?

Consider weather impacts on supply/demand, forecast trends, and risk factors. Respond with only "yes" or "no"."""

    try:
        response = requests.post(
            f"http://{OLLAMA_HOST}:{OLLAMA_PORT}/api/generate", 
            json={"model": "llama3.2:1b", "prompt": prompt, "stream": False},
            timeout=30
        )
        response.raise_for_status()
        
        response_data = response.json()
        if 'response' in response_data:
            decision = response_data['response'].strip().lower()
            should_trade = 'yes' in decision
            context.log.info(f"Should we trade 🤖📈?: {decision[:100]}")
            return should_trade
        else:
            context.log.warning(f"Ollama response missing 'response' key: {response_data}")
    except Exception as e:
        context.log.warning(f"Ollama error: {e}. Using fallback rule-based decision.")
    
    # Fallback: simple rule-based decision
    should_trade = predicted_price > 50
    context.log.info(f"Fallback decision: {'Trade' if should_trade else 'Do not trade'}")
    return should_trade


def _store_decision_in_delta_lake(
    context: AssetExecutionContext,
    predicted_price: float,
    should_trade: bool
) -> None:
    """Store the trading decision in Delta Lake and register in Hive Metastore"""
    try:
        namespace = "analytics"
        table_name = "trading_decisions"
        storage_location = f"abfss://{AZURE_STORAGE_CONTAINER}@{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{table_name}"
        
        # Create DataFrame with the decision
        df = pd.DataFrame([{
            'timestamp': datetime.now(),
            'predicted_price': float(predicted_price),
            'decision': bool(should_trade)
        }])
        
        context.log.info(f"📝 Writing decision to Delta Lake at {storage_location}")
        
        # Configure Azure storage credentials for Delta Lake
        storage_options = {
            "AZURE_STORAGE_ACCOUNT_NAME": AZURE_STORAGE_ACCOUNT_NAME,
            "AZURE_STORAGE_ACCOUNT_KEY": AZURE_STORAGE_ACCOUNT_KEY
        }
        
        # Write to Delta Lake (append mode)
        write_deltalake(
            storage_location,
            df,
            mode="append",
            storage_options=storage_options,
            partition_by=None
        )
        
        context.log.info(f"✅ Stored decision in Delta Lake: price=${predicted_price:.2f}, trade={should_trade}")
        
        # Register in Hive Metastore
        _register_in_hive_metastore(context, namespace, table_name, storage_location)
        
    except Exception as e:
        context.log.error(f"Delta Lake error: {e}. Decision not stored in lakehouse.")


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
        
        # Check if table exists
        try:
            existing_table = client.get_table(dbname=namespace, tbl_name=table_name)
            if existing_table:
                context.log.info(f"Table {namespace}.{table_name} already registered")
                client.close()
                return
        except Exception:
            context.log.info(f"Table {namespace}.{table_name} does not exist, creating it")
        
        # Define table schema
        cols = [
            FieldSchema(name='timestamp', type='timestamp', comment='Decision timestamp'),
            FieldSchema(name='predicted_price', type='double', comment='Predicted energy price'),
            FieldSchema(name='decision', type='boolean', comment='Trading decision')
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
        
        client.close()
        
    except ImportError:
        context.log.warning("hmsclient not installed, skipping Hive Metastore registration")
    except Exception as e:
        context.log.warning(f"Could not register table in Hive Metastore: {e}")


def _store_decision_in_starrocks(
    context: AssetExecutionContext,
    predicted_price: float,
    should_trade: bool
) -> None:
    """Store the trading decision in StarRocks database"""
    with get_starrocks_connection() as (conn, cursor):
        # Create database if not exists
        cursor.execute("CREATE DATABASE IF NOT EXISTS energy_trading")
        cursor.execute("USE energy_trading")
        
        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS trading_decisions (
            id BIGINT,
            timestamp DATETIME,
            predicted_price FLOAT,
            decision BOOLEAN
        ) ENGINE=OLAP
        DUPLICATE KEY(id, timestamp)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1")
        """)
        
        # Generate ID manually
        cursor.execute("SELECT IFNULL(MAX(id), 0) + 1 FROM trading_decisions")
        next_id = cursor.fetchone()[0]
        
        cursor.execute(
            "INSERT INTO trading_decisions (id, timestamp, predicted_price, decision) VALUES (%s, NOW(), %s, %s)", 
            (int(next_id), float(predicted_price), bool(should_trade))
        )
        
        conn.commit()
        context.log.info(f"✅ Stored decision in StarRocks: price=${predicted_price:.2f}, trade={should_trade}")
