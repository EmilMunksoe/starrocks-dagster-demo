"""Energy trading pipeline - Dagster definitions"""

from dagster import (
    Definitions,
    ScheduleDefinition,
    AssetSelection,
    DefaultScheduleStatus,
)

from .assets import all_assets

# Schedule to run entire pipeline every 15 minutes
trading_schedule = ScheduleDefinition(
    name="trading_decision_schedule",
    target=AssetSelection.all(),
    cron_schedule="*/15 * * * *",  # Every 15 minutes
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=all_assets,
    schedules=[trading_schedule],
)
