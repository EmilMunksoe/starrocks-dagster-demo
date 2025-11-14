# Energy Trading Pipeline Demo

A modern data lakehouse demo showcasing **Dagster**, **StarRocks**, **Delta Lake**, and **Ollama AI** for energy trading analytics.

## Features

- **Delta Lake** on Azure Storage with Hive Metastore catalog
- **StarRocks** multi-catalog analytics (Hive, PostgreSQL, native OLAP)
- **ML-powered** energy price predictions with scikit-learn
- **AI trading decisions** using Ollama LLM
- **Multi-catalog queries** across heterogeneous data sources via single SQL endpoint

## Quick Start

```bash
# Start all services
docker-compose up --build

# Access Dagster UI
open http://localhost:3000

# Query StarRocks
mysql -h localhost -P 9030 -u root
```

## Architecture

```
Weather Data → Delta Lake (Azure) → Hive Metastore
                    ↓
            ML Training (scikit-learn)
                    ↓
            AI Decision (Ollama) → StarRocks Multi-Catalog
                                    ├─ Native Tables
                                    ├─ Hive Catalog (Delta Lake)
                                    └─ PostgreSQL Catalog (Metadata)
```

## Project Structure

- `dagster/` - Dagster orchestration pipeline ([see detailed README](dagster/README.md))
- `metastore/` - Hive Metastore service configuration
- `docker-compose.yml` - Full stack deployment

## Environment Setup

Copy `.env.example` to `.env` and configure:

```bash
AZURE_STORAGE_ACCOUNT_NAME=your_account
AZURE_STORAGE_ACCOUNT_KEY=your_key
AZURE_STORAGE_CONTAINER=weather-data
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Dagster UI | 3000 | Pipeline orchestration & monitoring |
| StarRocks MySQL | 9030 | SQL query interface |
| StarRocks HTTP | 8030 | Admin interface |
| Hive Metastore | 9083 | Catalog service |
| PostgreSQL | 5432 | Metastore backend |
| Ollama | 11434 | LLM API |
