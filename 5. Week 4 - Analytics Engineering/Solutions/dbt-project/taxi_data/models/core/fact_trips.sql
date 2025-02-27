{{
    config(
        materialized='table'
    )
}}

WITH green_tripdata AS (
    SELECT *, 
        'Green' AS service_type
    FROM {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata AS (
    SELECT *, 
        'Yellow' AS service_type
    FROM {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned AS (
    SELECT * FROM green_tripdata
    UNION ALL 
    SELECT * FROM yellow_tripdata
), 
dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)
SELECT trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 
    trips_unioned.pickup_locationid, 
    COALESCE(pickup_zone.borough, 'Unknown') AS pickup_borough, 
    COALESCE(pickup_zone.zone, 'Unknown') AS pickup_zone, 
    trips_unioned.dropoff_locationid,
    COALESCE(dropoff_zone.borough, 'Unknown') AS dropoff_borough, 
    COALESCE(dropoff_zone.zone, 'Unknown') AS dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description
FROM trips_unioned
LEFT JOIN dim_zones AS pickup_zone
ON trips_unioned.pickup_locationid = pickup_zone.locationid
LEFT JOIN dim_zones AS dropoff_zone
ON trips_unioned.dropoff_locationid = dropoff_zone.locationid