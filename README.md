# Energy Trading Demo with Dagster, StarRocks, Delta Lake, and Ollama

This demo showcases an ETL pipeline for energy trading decisions using weather data. The pipeline reads weather data from Delta Lake tables stored directly in Azure Blob Storage (not Databricks), trains a machine learning model, runs inference, and uses Ollama AI to make trading decisions. Results are stored in StarRocks. Data governance is handled via the open-source Unity Catalog.

https://github.com/StarRocks/starrocks/issues/54969
