"""
Script to manually create a Delta table in Azure Storage Account
Run this locally before starting the demo if you want pre-existing data
"""
import pandas as pd
from deltalake import write_deltalake
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()

AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_STORAGE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "weather-data")

# Create sample weather data
data = {
    'timestamp': pd.date_range('2025-01-01', periods=100, freq='h'),
    'temperature': [20 + i % 15 for i in range(100)],
    'humidity': [50 + i % 40 for i in range(100)],
    'wind_speed': [5 + i % 15 for i in range(100)],
    'energy_price': [45 + i % 20 for i in range(100)]
}
df = pd.DataFrame(data)

# Create container if not exists
account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)

try:
    blob_service_client.create_container(AZURE_STORAGE_CONTAINER)
    print(f"Created container: {AZURE_STORAGE_CONTAINER}")
except Exception as e:
    print(f"Container already exists or error: {e}")

# Delta table path using Azure storage
storage_options = {
    "AZURE_STORAGE_ACCOUNT_NAME": AZURE_STORAGE_ACCOUNT_NAME,
    "AZURE_STORAGE_ACCOUNT_KEY": AZURE_STORAGE_ACCOUNT_KEY
}

delta_path = f"az://{AZURE_STORAGE_CONTAINER}/weather"

# Write as Delta table
write_deltalake(
    delta_path,
    df,
    mode='overwrite',
    storage_options=storage_options
)

print(f"✅ Created Delta table at: {delta_path}")
print(f"📊 Records written: {len(df)}")
print(f"\nYou can now run: docker-compose up --build")
