"""Configuration and environment variables for the energy trading pipeline"""

import os

# Azure Storage configuration
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_STORAGE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "weather-data")

# StarRocks configuration
STARROCKS_HOST = os.getenv("STARROCKS_HOST", "starrocks")
STARROCKS_PORT = int(os.getenv("STARROCKS_PORT", "9030"))
STARROCKS_USER = os.getenv("STARROCKS_USER", "root")
STARROCKS_PASSWORD = os.getenv("STARROCKS_PASSWORD", "root")

# Ollama configuration
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "ollama")
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434")

# Hive Metastore configuration
HIVE_METASTORE_HOST = os.getenv("HIVE_METASTORE_HOST", "hive-metastore")
HIVE_METASTORE_PORT = int(os.getenv("HIVE_METASTORE_PORT", "9083"))
