WITH quarterly_revenue AS (
    SELECT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        SUM(total_amount) AS quarterly_revenue
    FROM fact_trips
    GROUP BY year, quarter
),

yoy_growth AS (
    SELECT
        q1.year,
        q1.quarter,
        q1.quarterly_revenue,
        q2.quarterly_revenue AS prev_year_revenue,
        -- Calculate Year-over-Year Growth %
        ROUND(
            (q1.quarterly_revenue - q2.quarterly_revenue) / NULLIF(q2.quarterly_revenue, 0) * 100, 2
        ) AS yoy_growth
    FROM quarterly_revenue q1
    LEFT JOIN quarterly_revenue q2
        -- ON q1.trip_type = q2.trip_type
        ON q1.year = q2.year + 1
        AND q1.quarter = q2.quarter
    -- WHERE q1.year = 2020 -- Only focus on 2020
)

SELECT * FROM yoy_growth
ORDER BY yoy_growth DESC;
--------------------------------------------------------------------------------------
WITH filtered_trips AS (
    SELECT
        service_type,  -- 'Green' or 'Yellow'
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM fact_trips
    WHERE 
        fare_amount > 0 
        AND trip_distance > 0 
        AND payment_type_description IN ('Cash', 'Credit Card')
),

percentile_fares AS (
    SELECT
        service_type,
        year,
        month,
        PERCENTILE_CONT(0.97) WITHIN GROUP (ORDER BY fare_amount) AS p97,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fare_amount) AS p95,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY fare_amount) AS p90
    FROM filtered_trips
    -- WHERE year = 2020 AND month = 4  -- April 2020
    GROUP BY service_type, year, month
)

SELECT * FROM percentile_fares;
--------------------------------------------------------------------------------------
WITH green_tripdata AS (
    SELECT *, 
        'Green' AS service_type
    FROM green_tripdata
), 
yellow_tripdata AS (
    SELECT *, 
        'Yellow' AS service_type
    FROM yellow_tripdata
), 
trips_unioned AS (
    SELECT vendorid
	      ,lpep_pickup_datetime AS pickup_datetime
		  ,lpep_dropoff_datetime AS dropoff_datetime
		  ,passenger_count
		  ,trip_distance
		  ,ratecodeid
		  ,store_and_fwd_flag
		  ,pulocationid AS pickup_locationid
		  ,dolocationid AS dropoff_locationid
		  ,payment_type
		  ,fare_amount
		  ,extra
		  ,mta_tax
		  ,tip_amount
		  ,tolls_amount
		  ,improvement_surcharge
		  ,total_amount
		  ,congestion_surcharge
		  ,trip_type
		  ,ehail_fee
		  ,service_type
	FROM green_tripdata
    UNION ALL 
    SELECT vendorid
	      ,tpep_pickup_datetime
		  ,tpep_dropoff_datetime
		  ,passenger_count
		  ,trip_distance
		  ,ratecodeid
		  ,store_and_fwd_flag
		  ,pulocationid
		  ,dolocationid
		  ,payment_type
		  ,fare_amount
		  ,extra
		  ,mta_tax
		  ,tip_amount
		  ,tolls_amount
		  ,improvement_surcharge
		  ,total_amount
		  ,congestion_surcharge
		  ,trip_type
		  ,ehail_fee
		  ,service_type
	FROM yellow_tripdata
), 
dim_zones AS (
    SELECT * FROM dim_zones
    WHERE borough != 'Unknown'
)
SELECT --trips_unioned.tripid, 
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
    trips_unioned.payment_type
    -- trips_unioned.payment_type_description
FROM trips_unioned
LEFT JOIN dim_zones AS pickup_zone
ON trips_unioned.pickup_locationid = pickup_zone.locationid
LEFT JOIN dim_zones AS dropoff_zone
ON trips_unioned.dropoff_locationid = dropoff_zone.locationid
--------------------------------------------------------------------------------------