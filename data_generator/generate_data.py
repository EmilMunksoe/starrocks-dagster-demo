import os
import pandas as pd
from faker import Faker
from deltalake import write_deltalake
from azure.storage.blob import BlobServiceClient
import time

# Environment variables
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_STORAGE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "weather-data")

def generate_weather_data(num_records=100):
    fake = Faker()
    data = []
    base_time = fake.date_time_this_year()
    
    for i in range(num_records):
        # Create realistic weather patterns with some correlation
        temp = 15 + (i % 20) + fake.random_int(-5, 5)
        humidity = 40 + (i % 50) + fake.random_int(-10, 10)
        wind = max(0, 5 + (i % 15) + fake.random_int(-3, 3))
        # Price somewhat correlated with temperature (higher temp = higher AC demand)
        price = 40 + (temp - 15) * 0.8 + fake.random_int(-5, 5)
        
        data.append({
            'timestamp': base_time + pd.Timedelta(hours=i),
            'temperature': max(0, min(40, temp)),  # Celsius, clamp 0-40
            'humidity': max(20, min(95, humidity)),  # %, clamp 20-95
            'wind_speed': max(0, min(25, wind)),  # km/h, clamp 0-25
            'energy_price': max(30, min(70, price))  # $/MWh, clamp 30-70
        })
    return pd.DataFrame(data)

def upload_to_delta(df):
    # Azure Blob Storage URL
    account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)
    
    # Create container if not exists
    try:
        blob_service_client.create_container(AZURE_STORAGE_CONTAINER)
        print(f"Created container: {AZURE_STORAGE_CONTAINER}")
    except:
        pass  # Container already exists
    
    # Delta table path - use az:// scheme
    delta_path = f"az://{AZURE_STORAGE_CONTAINER}/weather"
    
    # Storage options for Azure
    storage_options = {
        "AZURE_STORAGE_ACCOUNT_NAME": AZURE_STORAGE_ACCOUNT_NAME,
        "AZURE_STORAGE_ACCOUNT_KEY": AZURE_STORAGE_ACCOUNT_KEY
    }
    
    # Write to Delta
    write_deltalake(delta_path, df, mode='append', storage_options=storage_options)
    print(f"Uploaded {len(df)} records to Delta table at {delta_path}")

if __name__ == "__main__":
    # Generate initial large dataset
    print("Generating initial weather dataset...")
    df = generate_weather_data(1000)  # Start with 1000 records
    upload_to_delta(df)
    print(f"Initial dataset uploaded: {len(df)} records")
    
    # Then continue generating smaller batches
    while True:
        df = generate_weather_data(50)  # Generate 50 records each time
        upload_to_delta(df)
        print(f"Data generated and uploaded. Total new records: {len(df)}. Sleeping for 300 seconds...")
        time.sleep(300)  # Every 5 minutes