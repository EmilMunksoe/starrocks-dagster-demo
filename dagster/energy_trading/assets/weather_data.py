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
)
from ..utils import register_delta_table_in_hive_metastore


class WeatherDataConfig(Config):
    """Configuration for weather data generation"""

    num_samples: int = Field(
        default=1500, description="Number of sample weather records to generate"
    )


@asset
def weather_data(
    context: AssetExecutionContext, config: WeatherDataConfig
) -> pd.DataFrame:
    """Generate weather data and write to Delta Lake with Hive Metastore registration"""

    namespace = "raw_data"
    table_name = "weather"
    storage_location = f"abfss://{AZURE_STORAGE_CONTAINER}@{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/weather"

    df = _generate_sample_data(context, config.num_samples)
    _write_to_delta_lake(context, df, storage_location)

    # Register in Hive Metastore using shared utility
    columns = [
        ("temperature", "double", "Temperature in Celsius"),
        ("humidity", "double", "Humidity percentage"),
        ("wind_speed", "double", "Wind speed in m/s"),
        ("energy_price", "double", "Energy price"),
    ]
    register_delta_table_in_hive_metastore(
        context, namespace, table_name, storage_location, columns, drop_if_exists=False
    )

    return df


def _generate_sample_data(
    context: AssetExecutionContext, num_samples: int
) -> pd.DataFrame:
    """Generate random sample weather data with significantly varied distributions"""
    import time

    np.random.seed(int(time.time() * 1000) % (2**32))

    # Randomly choose a "season" or "weather pattern" for this batch
    # This creates drastically different statistical distributions each run
    pattern = np.random.choice(["winter", "spring", "summer", "fall", "extreme"])

    if pattern == "winter":
        temp_mean, temp_std = 5, 8
        humidity_mean, humidity_std = 75, 15
        wind_mean, wind_std = 15, 8
        price_base = 90
        context.log.info("🌨️  Generating WINTER weather pattern")
    elif pattern == "summer":
        temp_mean, temp_std = 32, 6
        humidity_mean, humidity_std = 45, 20
        wind_mean, wind_std = 8, 5
        price_base = 110
        context.log.info("☀️  Generating SUMMER weather pattern")
    elif pattern == "spring":
        temp_mean, temp_std = 18, 7
        humidity_mean, humidity_std = 60, 18
        wind_mean, wind_std = 12, 6
        price_base = 60
        context.log.info("🌸  Generating SPRING weather pattern")
    elif pattern == "fall":
        temp_mean, temp_std = 15, 8
        humidity_mean, humidity_std = 68, 17
        wind_mean, wind_std = 13, 7
        price_base = 70
        context.log.info("🍂  Generating FALL weather pattern")
    else:  # extreme
        temp_mean, temp_std = np.random.uniform(-5, 45), 12
        humidity_mean, humidity_std = np.random.uniform(20, 90), 25
        wind_mean, wind_std = np.random.uniform(5, 25), 10
        price_base = np.random.uniform(40, 150)
        context.log.info("⚡ Generating EXTREME/RANDOM weather pattern")

    temperatures = np.random.normal(temp_mean, temp_std, num_samples)
    temperatures = np.clip(temperatures, -10, 50)

    humidities = np.random.normal(humidity_mean, humidity_std, num_samples)
    humidities = np.clip(humidities, 10, 100)

    wind_speeds = np.random.normal(wind_mean, wind_std, num_samples)
    wind_speeds = np.clip(wind_speeds, 0, 35)

    temp_effect = (temperatures - 20) * np.random.uniform(1.5, 3.5)
    wind_effect = wind_speeds * np.random.uniform(-2.0, -0.5)
    humidity_effect = (humidities - 50) * np.random.uniform(-0.3, 0.3)

    energy_prices = price_base + temp_effect + wind_effect + humidity_effect
    energy_prices += np.random.normal(0, 15, num_samples)  # Random noise
    energy_prices = np.clip(energy_prices, 20, 200)

    df = pd.DataFrame(
        {
            "temperature": temperatures,
            "humidity": humidities,
            "wind_speed": wind_speeds,
            "energy_price": energy_prices,
        }
    )

    context.log.info(
        f"Generated {len(df)} records | "
        f"Temp: {temperatures.mean():.1f}±{temperatures.std():.1f}°C | "
        f"Price: {energy_prices.mean():.1f}±{energy_prices.std():.1f}"
    )
    return df


def _write_to_delta_lake(
    context: AssetExecutionContext, df: pd.DataFrame, storage_location: str
) -> None:
    """Write DataFrame to Delta Lake format on Azure Storage"""
    try:
        storage_options = {
            "AZURE_STORAGE_ACCOUNT_NAME": AZURE_STORAGE_ACCOUNT_NAME,
            "AZURE_STORAGE_ACCOUNT_KEY": AZURE_STORAGE_ACCOUNT_KEY,
        }

        context.log.info(
            f"Writing {len(df)} records as Delta Lake to {storage_location}"
        )
        write_deltalake(
            storage_location, df, mode="append", storage_options=storage_options
        )
        context.log.info("Successfully wrote Delta Lake data to Azure Storage")

    except Exception as e:
        context.log.error(f"Failed to write Delta Lake data: {e}")
        raise
