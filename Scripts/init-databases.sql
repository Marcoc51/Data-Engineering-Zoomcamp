CREATE TABLE nyc_taxi_data (
    VendorID INT,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP,
    store_and_fwd_flag CHAR(1),
    RatecodeID INT,
    PULocationID INT,
    DOLocationID INT,
    passenger_count BIGINT,
    trip_distance NUMERIC(10, 2),
    fare_amount NUMERIC(10, 2),
    extra NUMERIC(10, 2),
    mta_tax NUMERIC(10, 2),
    tip_amount NUMERIC(10, 2),
    tolls_amount NUMERIC(10, 2),
    ehail_fee NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    payment_type INT,
    trip_type INT,
    congestion_surcharge NUMERIC(10, 2)
);

CREATE TABLE taxi_zone_lookup (
    LocationID INT PRIMARY KEY,
    Borough VARCHAR(100),
    Zone VARCHAR(255),
    service_zone VARCHAR(100)
);

CREATE TABLE "yellow_trip_data" (
  "index" INTEGER,
  "VendorID" REAL,
  "tpep_pickup_datetime" TIMESTAMP,
  "tpep_dropoff_datetime" TIMESTAMP,
  "passenger_count" REAL,
  "trip_distance" REAL,
  "RatecodeID" REAL,
  "store_and_fwd_flag" TEXT,
  "PULocationID" REAL,
  "DOLocationID" REAL,
  "payment_type" REAL,
  "fare_amount" REAL,
  "extra" REAL,
  "mta_tax" REAL,
  "tip_amount" REAL,
  "tolls_amount" REAL,
  "improvement_surcharge" REAL,
  "total_amount" REAL,
  "congestion_surcharge" REAL,
  "Airport_fee" REAL
);
