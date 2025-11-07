# Project Structure

## Overview
The energy trading pipeline has been refactored for better modularity and maintainability. Each asset now lives in its own file with clear separation of concerns.

## Directory Structure

```
dagster/
├── energy_trading/
│   ├── __init__.py              # Main definitions and job configuration
│   ├── config.py                # Centralized environment variables and configuration
│   ├── assets/                  # Asset definitions (one file per asset)
│   │   ├── __init__.py          # Exports all assets
│   │   ├── weather_data.py      # Weather data loading and Unity Catalog registration
│   │   ├── trained_model.py     # ML model training
│   │   └── trading_decision.py  # Trading decision logic with Ollama AI
│   └── assets.py                # (Old monolithic file - can be removed)
```

## Asset Configuration

### weather_data
**Location:** `assets/weather_data.py`

**Configuration:**
```json
{
  "num_samples": 100
}
```

The `num_samples` parameter is now properly configured as a Pydantic `Config` class, which means:
- ✅ Shows up in Dagster UI with proper defaults
- ✅ Includes description/documentation
- ✅ Type-safe with validation
- ✅ Easy to configure in ad-hoc materializations

**Default:** 25 samples

**Functions:**
- `weather_data()` - Main asset function
- `_generate_sample_data()` - Generates random weather data
- `_register_in_unity_catalog()` - Registers table in Unity Catalog
- `_create_catalog()` - Creates catalog
- `_create_schema()` - Creates schema
- `_register_table()` - Registers table with columns

### trained_model
**Location:** `assets/trained_model.py`

Trains a linear regression model to predict energy prices based on weather conditions.

**Dependencies:** `weather_data`

### trading_decision
**Location:** `assets/trading_decision.py`

Makes trading decisions using Ollama AI and stores results in StarRocks.

**Dependencies:** `weather_data`, `trained_model`

**Functions:**
- `trading_decision()` - Main asset function
- `_get_latest_weather_data()` - Gets or generates weather data
- `_get_trading_decision_from_ollama()` - Calls Ollama AI for decision
- `_store_decision_in_starrocks()` - Persists decision to database

## Configuration (config.py)

All environment variables are centralized in `config.py`:
- Azure Storage credentials and container name
- StarRocks connection details
- Ollama API endpoint
- Unity Catalog API endpoint

## Benefits of This Structure

1. **Modularity** - Each asset is self-contained
2. **Maintainability** - Easy to find and update specific logic
3. **Testability** - Each asset and its helper functions can be tested independently
4. **Documentation** - Clear separation makes it easier to understand
5. **Configuration** - Proper Pydantic Config classes provide type safety and UI integration
6. **Reusability** - Helper functions can be extracted to shared utilities if needed

## Usage

### Running Individual Assets

In Dagster UI:
1. Navigate to Assets tab
2. Select `weather_data`
3. Click "Materialize selected"
4. Configure if needed:
   ```json
   {
     "num_samples": 50
   }
   ```

### Running the Full Pipeline

Use the `energy_trading_pipeline` job to run all assets in dependency order.

## Migration Notes

The old `assets.py` file has been replaced with:
- `config.py` - Configuration
- `assets/weather_data.py` - Weather data asset
- `assets/trained_model.py` - Model training asset
- `assets/trading_decision.py` - Trading decision asset
- `assets/__init__.py` - Asset exports

The old `assets.py` file can be safely removed after verifying the new structure works correctly.
