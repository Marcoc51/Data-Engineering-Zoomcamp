import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
engine.connect()

df_data = pd.read_csv("Week 1 - Docker, SQL & Terraform\data\green_tripdata_2019-10.csv")
df_data.to_sql(name='nyc_taxi_data', con=engine, if_exists='replace')

df_zones = pd.read_csv("Week 1 - Docker, SQL & Terraform\data\\taxi_zone_lookup.csv")
df_zones.to_sql(name='taxi_zone_lookup', con=engine, if_exists='replace')