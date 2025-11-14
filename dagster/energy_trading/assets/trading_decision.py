"""Trading decision asset - uses Ollama AI to make trading decisions"""

from datetime import datetime, timedelta
from typing import Dict, Any

import numpy as np
import pandas as pd
import requests
from dagster import asset, AssetExecutionContext
from sklearn.linear_model import LinearRegression
from deltalake import write_deltalake

from ..config import (
    OLLAMA_HOST,
    OLLAMA_PORT,
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_STORAGE_ACCOUNT_KEY,
    AZURE_STORAGE_CONTAINER,
)
from ..utils import get_starrocks_connection, register_delta_table_in_hive_metastore


@asset
def trading_decision(
    context: AssetExecutionContext,
    weather_data: pd.DataFrame,
    trained_model: LinearRegression,
) -> Dict[str, Any]:
    """Run model on latest data and use Ollama to decide trading"""

    latest_data = _get_latest_weather_data(context, weather_data)

    predicted_price = trained_model.predict(latest_data)[0]
    context.log.info(f"Predicted energy price: ${predicted_price:.2f}/MWh")

    should_trade = _get_trading_decision_from_ollama(
        context, latest_data, predicted_price
    )

    _store_decision_in_delta_lake(context, predicted_price, should_trade)

    # Also store in StarRocks (for fast querying)
    _store_decision_in_starrocks(context, predicted_price, should_trade)

    return {"predicted_price": predicted_price, "should_trade": should_trade}


def _get_latest_weather_data(
    context: AssetExecutionContext, weather_data: pd.DataFrame
) -> pd.DataFrame:
    """Get the latest weather data or generate test data"""
    if len(weather_data) > 1:
        latest_data = weather_data.iloc[-1:][["temperature", "humidity", "wind_speed"]]
        context.log.info("Using latest weather data for prediction")
    else:
        test_temp = np.random.uniform(15, 30)
        test_humidity = np.random.uniform(40, 80)
        test_wind = np.random.uniform(1, 15)
        latest_data = pd.DataFrame(
            {
                "temperature": [test_temp],
                "humidity": [test_humidity],
                "wind_speed": [test_wind],
            }
        )
        context.log.info(
            f"Using generated test data: temp={test_temp:.1f}°C, "
            f"humidity={test_humidity:.1f}%, wind={test_wind:.1f}m/s"
        )

    return latest_data


def _get_trading_decision_from_ollama(
    context: AssetExecutionContext, latest_data: pd.DataFrame, predicted_price: float
) -> bool:
    """Use Ollama AI to decide whether to trade"""
    current_weather = latest_data.iloc[0]
    temp = current_weather["temperature"]
    humidity = current_weather["humidity"]
    wind_speed = current_weather["wind_speed"]

    # Generate forecast
    current_time = datetime.now()
    forecast_temp = temp + np.random.normal(0, 2)
    forecast_humidity = humidity + np.random.normal(0, 5)
    forecast_wind = wind_speed + np.random.normal(0, 2)
    forecast_time = current_time + timedelta(hours=6)

    market_baseline = 55.0
    price_diff = predicted_price - market_baseline

    prompt = f"""You are an AI energy trading analyst. Analyze the following data and decide whether to execute a trade:

CURRENT CONDITIONS ({current_time.strftime("%Y-%m-%d %H:%M")}):
- Temperature: {temp:.1f}°C
- Humidity: {humidity:.1f}%
- Wind Speed: {wind_speed:.1f} m/s
- ML Predicted Price: ${predicted_price:.2f}/MWh (vs market baseline ${market_baseline:.2f}/MWh = {price_diff:+.2f} difference)

6-HOUR FORECAST ({forecast_time.strftime("%Y-%m-%d %H:%M")}):
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
            timeout=30,
        )
        response.raise_for_status()

        response_data = response.json()
        if "response" in response_data:
            decision = response_data["response"].strip().lower()
            should_trade = "yes" in decision
            context.log.info(f"Should we trade 🤖📈?: {decision[:100]}")
            return should_trade
        else:
            context.log.warning(
                f"Ollama response missing 'response' key: {response_data}"
            )
    except Exception as e:
        context.log.warning(f"Ollama error: {e}. Using fallback rule-based decision.")

    # Fallback: simple rule-based decision
    should_trade = predicted_price > 50
    context.log.info(
        f"Fallback decision: {'Trade' if should_trade else 'Do not trade'}"
    )
    return should_trade


def _store_decision_in_delta_lake(
    context: AssetExecutionContext, predicted_price: float, should_trade: bool
) -> None:
    """Store the trading decision in Delta Lake and register in Hive Metastore"""
    try:
        namespace = "analytics"
        table_name = "trading_decisions"
        storage_location = f"abfss://{AZURE_STORAGE_CONTAINER}@{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{table_name}"

        df = pd.DataFrame(
            [
                {
                    "timestamp": datetime.now(),
                    "predicted_price": float(predicted_price),
                    "decision": bool(should_trade),
                }
            ]
        )

        context.log.info(f"📝 Writing decision to Delta Lake at {storage_location}")

        storage_options = {
            "AZURE_STORAGE_ACCOUNT_NAME": AZURE_STORAGE_ACCOUNT_NAME,
            "AZURE_STORAGE_ACCOUNT_KEY": AZURE_STORAGE_ACCOUNT_KEY,
        }

        # Write to Delta Lake (append mode)
        write_deltalake(
            storage_location,
            df,
            mode="append",
            storage_options=storage_options,
            partition_by=None,
        )

        context.log.info(
            f"✅ Stored decision in Delta Lake: price=${predicted_price:.2f}, trade={should_trade}"
        )

        columns = [
            ("timestamp", "timestamp", "Decision timestamp"),
            ("predicted_price", "double", "Predicted energy price"),
            ("decision", "boolean", "Trading decision"),
        ]
        register_delta_table_in_hive_metastore(
            context,
            namespace,
            table_name,
            storage_location,
            columns,
            drop_if_exists=False,
        )

    except Exception as e:
        context.log.error(f"Delta Lake error: {e}. Decision not stored in lakehouse.")


def _store_decision_in_starrocks(
    context: AssetExecutionContext, predicted_price: float, should_trade: bool
) -> None:
    """Store the trading decision in StarRocks database"""
    with get_starrocks_connection() as (conn, cursor):
        cursor.execute("CREATE DATABASE IF NOT EXISTS energy_trading")
        cursor.execute("USE energy_trading")

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

        cursor.execute("SELECT IFNULL(MAX(id), 0) + 1 FROM trading_decisions")
        next_id = cursor.fetchone()[0]

        cursor.execute(
            "INSERT INTO trading_decisions (id, timestamp, predicted_price, decision) VALUES (%s, NOW(), %s, %s)",
            (int(next_id), float(predicted_price), bool(should_trade)),
        )

        conn.commit()
        context.log.info(
            f"✅ Stored decision in StarRocks: price=${predicted_price:.2f}, trade={should_trade}"
        )
