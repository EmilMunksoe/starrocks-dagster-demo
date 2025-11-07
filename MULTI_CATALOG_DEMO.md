# Multi-Catalog Analytics Demo

This project demonstrates StarRocks' ability to query multiple data sources through a **single MySQL endpoint**.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              StarRocks MySQL Endpoint (localhost:9030)       │
└─────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   ┌────▼─────┐        ┌──────▼──────┐      ┌──────▼──────┐
   │ default_ │        │    hive_    │      │  postgres_  │
   │ catalog  │        │   catalog   │      │   catalog   │
   └──────────┘        └─────────────┘      └─────────────┘
        │                     │                     │
   ┌────▼─────┐        ┌──────▼──────┐      ┌──────▼──────┐
   │ StarRocks│        │ Delta Lake  │      │ PostgreSQL  │
   │  Native  │        │   + Hive    │      │    Hive     │
   │   OLAP   │        │  Metastore  │      │  Metastore  │
   └──────────┘        └─────────────┘      └─────────────┘
```

## Data Sources

### 1. Default Catalog (StarRocks Native)
- **Type**: StarRocks OLAP storage
- **Database**: `energy_trading`
- **Table**: `trading_decisions`
- **Purpose**: Fast analytics on trading decisions
- **Query**: `SELECT * FROM default_catalog.energy_trading.trading_decisions`

### 2. Hive Catalog (Delta Lake)
- **Type**: External Hive Metastore + Delta Lake
- **Database**: `raw_data`, `analytics`
- **Tables**: `weather`, `trading_decisions`
- **Storage**: Azure Blob Storage (ABFSS)
- **Purpose**: Query data lake files without ETL
- **Query**: `SELECT * FROM hive_catalog.raw_data.weather`

### 3. PostgreSQL Catalog (Hive Metastore Backend)
- **Type**: JDBC connection to PostgreSQL
- **Database**: `public` (metastore schema)
- **Tables**: `DBS`, `TBLS`, `COLUMNS_V2`, etc.
- **Purpose**: Query metadata about Hive tables
- **Query**: `SELECT * FROM postgres_catalog.public.DBS`

## Dagster Pipeline

The pipeline creates the multi-catalog setup:

1. **weather_data** → Generates weather data, writes to Delta Lake, registers in Hive
2. **trading_decision** → AI trading decisions, writes to Delta Lake + StarRocks
3. **delta_external_catalog** → Creates Hive external catalog in StarRocks
4. **postgres_external_catalog** → Creates PostgreSQL external catalog in StarRocks
5. **multi_catalog_analytics** → Joins data from all 3 catalogs into one table

## Running the Demo

### 1. Start Services
```bash
docker-compose up -d
```

### 2. Materialize Assets in Dagster
1. Open Dagster UI: http://localhost:3000
2. Navigate to Assets
3. Select all assets and click "Materialize"

### 3. Query via MySQL Client
```bash
# Connect to StarRocks
mysql -h localhost -P 9030 -u root

# Show all catalogs
SHOW CATALOGS;

# Query native catalog
SET CATALOG default_catalog;
USE energy_trading;
SELECT * FROM trading_decisions LIMIT 5;

# Query Hive catalog (Delta Lake)
SET CATALOG hive_catalog;
USE raw_data;
SELECT * FROM weather LIMIT 5;

# Query PostgreSQL catalog (Metastore metadata)
SET CATALOG postgres_catalog;
USE public;
SELECT NAME, DB_LOCATION_URI FROM DBS;
SELECT TBL_NAME FROM TBLS;

# Query the multi-catalog analytics table
SET CATALOG default_catalog;
USE energy_trading;
SELECT * FROM decision_weather_analytics;
```

### 4. Run Demo Script
```bash
chmod +x test_multi_catalog.sh
./test_multi_catalog.sh
```

## Cross-Catalog JOIN Example

```sql
-- Join data from all three catalogs in a single query
SELECT 
    td.id,
    td.predicted_price,
    td.should_trade,
    AVG(w.temperature) as avg_temp,
    AVG(w.energy_price) as avg_energy_price,
    (SELECT COUNT(*) FROM postgres_catalog.public.DBS) as total_hive_databases
FROM default_catalog.energy_trading.trading_decisions td
CROSS JOIN hive_catalog.raw_data.weather w
GROUP BY td.id, td.predicted_price, td.should_trade
LIMIT 10;
```

## Key Benefits

✅ **Single Endpoint**: One MySQL connection queries all data sources  
✅ **No ETL**: Query data where it lives (lake, OLAP, relational)  
✅ **Real-time**: No data movement or replication  
✅ **SQL Standard**: Use familiar SQL syntax  
✅ **Cross-Catalog JOINs**: Combine data from different sources seamlessly  

## Technologies

- **StarRocks 3.5**: Multi-catalog query engine
- **Apache Hive Metastore 4.0**: Metadata management
- **Delta Lake**: Open table format on Azure Storage
- **PostgreSQL 15**: Metastore backend
- **Dagster**: Data pipeline orchestration
- **Ollama**: AI model for trading decisions
