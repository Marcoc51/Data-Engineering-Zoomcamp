-------------------------------------------------------------------------------------------------------------------------
-- Question 3. Trip Segmentation Count
SELECT 
    COUNT(*) FILTER (WHERE trip_distance <= 1) AS "Up to 1 mile",
    COUNT(*) FILTER (WHERE trip_distance > 1 AND trip_distance <= 3) AS "1 to 3 miles",
    COUNT(*) FILTER (WHERE trip_distance > 3 AND trip_distance <= 7) AS "3 to 7 miles",
    COUNT(*) FILTER (WHERE trip_distance > 7 AND trip_distance <= 10) AS "7 to 10 miles",
    COUNT(*) FILTER (WHERE trip_distance > 10) AS "Over 10 miles"
FROM public.nyc_taxi_data
WHERE lpep_pickup_datetime >= '2019-10-01' AND lpep_pickup_datetime < '2019-11-01';

-------------------------------------------------------------------------------------------------------------------------
-- Question 4. Longest trip for each day
SELECT
    DATE(lpep_pickup_datetime) AS pickup_date,
    MAX(trip_distance) AS longest_trip
FROM public.nyc_taxi_data
WHERE lpep_pickup_datetime >= '2019-10-01' AND lpep_pickup_datetime < '2019-11-01'
GROUP BY lpep_pickup_datetime
ORDER BY longest_trip DESC
LIMIT 1;

-------------------------------------------------------------------------------------------------------------------------
-- Question 5. Three biggest pickup zones
SELECT 
    z."Zone" AS pickup_zone, 
    SUM(t.total_amount) AS total_amount
FROM public.nyc_taxi_data AS t
JOIN public.taxi_zone_lookup AS z ON t."PULocationID" = z."LocationID"
WHERE DATE(t.lpep_pickup_datetime) = '2019-10-18'
GROUP BY z."Zone"
HAVING SUM(t.total_amount) > 13000
ORDER BY total_amount DESC;

-------------------------------------------------------------------------------------------------------------------------
-- Question 6. Largest tip
SELECT 
    z2."Zone" AS dropoff_zone, 
    MAX(t.tip_amount) AS largest_tip
FROM public.nyc_taxi_data AS t
JOIN public.taxi_zone_lookup AS z1 ON t."PULocationID" = z1."LocationID"
JOIN public.taxi_zone_lookup AS z2 ON t."DOLocationID" = z2."LocationID"
WHERE DATE(t.lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31'
  AND z1."Zone" = 'East Harlem North'
GROUP BY z2."Zone"
ORDER BY largest_tip DESC
LIMIT 1;

-------------------------------------------------------------------------------------------------------------------------