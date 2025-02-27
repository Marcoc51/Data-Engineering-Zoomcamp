import pandas as pd
import gzip
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
DB_HOST = "localhost"
DB_PORT = os.getenv("HOST_PORT")
DB_NAME = "taxi_db"
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Define the base data folder
base_folder = "5. Week 4 - Analytics Engineering/Data"

# Mapping of subfolders to PostgreSQL tables
subfolders = {
    # "Green NY Taxi data 2019-2020": "green_taxi_db",
    "Yellow NY Taxi data 2019-2020": "yellow_taxi_db",
    "FHV NY Taxi data 2019": "fhv_taxi_db"
}

# Define chunk size (adjust based on memory)
CHUNK_SIZE = 100_000  # Process 100,000 rows at a time

# Function to read and insert CSV data in chunks
def load_csv_in_chunks(file_path, table_name, engine):
    with gzip.open(file_path, 'rt') as f:  # Open compressed CSV
        chunk_iter = pd.read_csv(f, chunksize=CHUNK_SIZE)  # Read in chunks

        for chunk_num, chunk in enumerate(chunk_iter):
            print(f"⏳ Loading chunk {chunk_num + 1} into {table_name}...")
            
            # Append data in chunks
            chunk.to_sql(
                table_name, engine, if_exists="append", index=False,
                method="multi", chunksize=CHUNK_SIZE
            )
            
            print(f"✅ Chunk {chunk_num + 1} loaded successfully.")

# Iterate through each subfolder
for subfolder, table_name in subfolders.items():
    folder_path = os.path.join(base_folder, subfolder)

    # PostgreSQL connection string
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # Create SQLAlchemy engine
    engine = create_engine(db_url)

    # Process files one by one
    for file_name in sorted(os.listdir(folder_path)):  # Sort for predictable order
        if file_name.endswith(".csv.gz"):
            file_path = os.path.join(folder_path, file_name)
            
            print("------------------------------------------------------------------")
            print(f"🚀 Processing {file_name} for database: {DB_NAME}, table: {table_name}")

            # Load data in chunks
            load_csv_in_chunks(file_path, table_name, engine)

            print(f"✅ Data from {file_name} fully loaded into {DB_NAME}.{table_name}")
            print("------------------------------------------------------------------")
