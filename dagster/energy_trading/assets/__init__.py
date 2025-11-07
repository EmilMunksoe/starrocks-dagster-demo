"""Asset definitions for the energy trading pipeline"""
from dagster import load_assets_from_modules

from . import (
    wearther_data, 
    trained_model, 
    trading_decision, 
    delta_external_catalog,
    postgres_external_catalog,
    multi_catalog_analytics
)

# Load all assets from the modules
weather_data_assets = load_assets_from_modules([wearther_data])
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
    *multi_catalog_analytics_assets
]
