import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb

# Get the version of the dlt package
print("------------------------------------------------------------------")
print("dlt version:", dlt.__version__)

duckdb_path = "Workshop 1 - Ingestion with dlt/Data/ny_taxi_pipeline.duckdb"

# Define the API resource for NYC taxi data
@dlt.resource(name="rides")   # <--- The name of the resource (will be used as the table name)
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page   # <--- yield data to manage memory

# define new dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data",
    pipelines_dir=duckdb_path
)

# run the pipeline with the new resource
load_info = pipeline.run(ny_taxi, write_disposition="replace")
print("------------------------------------------------------------------")
print(load_info)


# explore loaded data
rides_df = pipeline.dataset(dataset_type="default").rides.df()
print("------------------------------------------------------------------")
print(rides_df)

# A database 'ny_taxi_pipeline.duckdb' was created in working directory so just connect to it

# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset
print("------------------------------------------------------------------")
print(conn.sql("DESCRIBE").df())

# Query the dataset to get the average trip duration in minutes
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    
    # Prints column values of the first row
    print("------------------------------------------------------------------")
    print(res)