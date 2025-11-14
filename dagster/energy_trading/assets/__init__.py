"""Asset definitions for the energy trading pipeline

This module contains all Dagster assets that make up the energy trading pipeline:
- weather_data: Generates weather data and stores in Delta Lake via Hive Metastore
- trained_model: Trains ML model to predict energy prices
- trading_decision: Uses AI to make trading decisions based on predictions
- delta_external_catalog: Creates Hive external catalog in StarRocks for Delta Lake access
- postgres_external_catalog: Creates PostgreSQL external catalog for Hive Metastore metadata
- multi_catalog_analytics: Demonstrates multi-catalog queries across all data sources
"""

from dagster import load_assets_from_modules

from . import (
    weather_data,
    trained_model,
    trading_decision,
    delta_external_catalog,
    postgres_external_catalog,
    multi_catalog_analytics,
)

# Load all assets from the modules
weather_data_assets = load_assets_from_modules([weather_data])
trained_model_assets = load_assets_from_modules([trained_model])
trading_decision_assets = load_assets_from_modules([trading_decision])
delta_external_catalog_assets = load_assets_from_modules([delta_external_catalog])
postgres_external_catalog_assets = load_assets_from_modules([postgres_external_catalog])
multi_catalog_analytics_assets = load_assets_from_modules([multi_catalog_analytics])

# Export all assets
all_assets = [
    *weather_data_assets,
    *trained_model_assets,
    *trading_decision_assets,
    *delta_external_catalog_assets,
    *postgres_external_catalog_assets,
    *multi_catalog_analytics_assets,
]
