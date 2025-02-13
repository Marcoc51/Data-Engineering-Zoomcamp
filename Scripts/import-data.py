import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
DB_HOST = "localhost"
DB_PORT = os.getenv("HOST_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Connect to the PostgreSQL database
try:
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    engine.connect()

    print("Connected to the database successfully!")

    # To get the schema of the files
    # print(pd.io.sql.get_schema(df_data.reset_index(), 'nyc_taxi_data', con=engine))

    df_data = pd.read_csv("Week 1 - Docker, SQL & Terraform\data\green_tripdata_2019-10.csv")
    df_data.to_sql(name='nyc_taxi_data', con=engine, if_exists='replace')

    df_zones = pd.read_csv("Week 1 - Docker, SQL & Terraform\data\\taxi_zone_lookup.csv")
    df_zones.to_sql(name='taxi_zone_lookup', con=engine, if_exists='replace')

    # Directory containing the Parquet files
    parquet_dir = "Week 3 - Data Warehouse\Data"

    # Iterate through all Parquet files in the directory
    for file in os.listdir(parquet_dir):
        if file.endswith(".parquet"):  # Process only Parquet files
            file_path = os.path.join(parquet_dir, file)
            
            # Read the Parquet file into a DataFrame
            df = pd.read_parquet(file_path)

            # Define the table name
            table_name = "yellow_trip_data"

            # Save DataFrame to PostgreSQL (append mode to avoid overwriting)
            df.to_sql(table_name, engine, if_exists="append", index=False)

            print(len(df))
            print(f"Saved {file} to table {table_name}")

    print("Data imported successfully")
    
    # engine.close()

except Exception as e:
    print("Error connecting to the database:", e)
