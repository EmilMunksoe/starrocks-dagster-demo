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
        default=350, description="Number of sample weather records to generate"
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
        context, namespace, table_name, storage_location, columns, drop_if_exists=True
    )

    return df


def _generate_sample_data(
    context: AssetExecutionContext, num_samples: int
) -> pd.DataFrame:
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

    df = pd.DataFrame(
        {
            "temperature": temperatures,
            "humidity": humidities,
            "wind_speed": wind_speeds,
            "energy_price": energy_prices,
        }
    )

    context.log.info(f"Generated {len(df)} random sample records")
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
            storage_location, df, mode="overwrite", storage_options=storage_options
        )
        context.log.info("Successfully wrote Delta Lake data to Azure Storage")

    except Exception as e:
        context.log.error(f"Failed to write Delta Lake data: {e}")
        raise
