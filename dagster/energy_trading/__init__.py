"""Energy trading pipeline - Dagster definitions"""
from dagster import Definitions, define_asset_job, AssetSelection
from .assets import all_assets

# Define a job that materializes all assets in dependency order
energy_trading_job = define_asset_job(
    name="energy_trading_pipeline",
    selection=AssetSelection.all(),
    description="Complete energy trading pipeline: load weather data, train model, make trading decision"
)

defs = Definitions(
    assets=all_assets,
    jobs=[energy_trading_job]
)
