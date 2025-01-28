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

-- COPY nyc_taxi_data FROM '\data\green_tripdata_2019-10.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE taxi_zone_lookup (
    LocationID INT PRIMARY KEY,
    Borough VARCHAR(100),
    Zone VARCHAR(255),
    service_zone VARCHAR(100)
);

-- COPY taxi_zone_lookup FROM '\data\taxi_zone_lookup.csv' DELIMITER ',' CSV HEADER;