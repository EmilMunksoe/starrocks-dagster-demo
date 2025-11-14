# Energy Trading Dagster Pipeline

This Dagster pipeline demonstrates a modern data lakehouse architecture for energy trading, featuring:

- **Delta Lake** storage on Azure with **Hive Metastore** catalog
- **StarRocks** multi-catalog analytics (Hive, PostgreSQL, native tables)
- **Machine Learning** for energy price prediction
- **AI-powered trading decisions** using Ollama

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Weather Data   │────▶│   Delta Lake     │────▶│  Hive Metastore │
│  Generation     │     │  (Azure Storage) │     │   Registration  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                                           │
                                                           ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   ML Training   │────▶│ Trading Decision │────▶│    StarRocks    │
│  (scikit-learn) │     │  (Ollama AI)     │     │  Multi-Catalog  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## Project Structure

```
dagster/
├── energy_trading/
│   ├── __init__.py              # Dagster definitions
│   ├── config.py                # Environment configuration
│   ├── utils.py                 # Shared utilities
│   └── assets/
│       ├── __init__.py          # Asset loading
│       ├── weather_data.py      # Generate & store weather data
│       ├── trained_model.py     # ML model training
│       ├── trading_decision.py  # AI trading decisions
│       ├── delta_external_catalog.py     # Hive catalog setup
│       ├── postgres_external_catalog.py  # PostgreSQL catalog setup
│       └── multi_catalog_analytics.py    # Cross-catalog queries
├── workspace.yaml               # Dagster workspace config
├── pyproject.toml              # Python dependencies
└── Dockerfile                  # Container definition
```

## Configuration

Set the following environment variables (typically in `.env` file):

```bash
# Azure Storage
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account
AZURE_STORAGE_ACCOUNT_KEY=your_storage_key
AZURE_STORAGE_CONTAINER=weather-data

# StarRocks
STARROCKS_HOST=starrocks
STARROCKS_PORT=9030
STARROCKS_USER=root
STARROCKS_PASSWORD=root

# Ollama
OLLAMA_HOST=ollama
OLLAMA_PORT=11434

# Hive Metastore
HIVE_METASTORE_HOST=hive-metastore
HIVE_METASTORE_PORT=9083
```

## Development

### Local Setup

1. Install dependencies with uv:
   ```bash
   uv sync
   ```

2. Run Dagster locally:
   ```bash
   dagster dev
   ```
