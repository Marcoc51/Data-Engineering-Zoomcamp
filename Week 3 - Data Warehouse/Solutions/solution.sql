-------------------------------------------------------------------------------------------------------------------------
-- Question 1. 
SELECT COUNT(*) 
FROM public.yellow_trip_data;

-------------------------------------------------------------------------------------------------------------------------
-- Question 2.
SELECT COUNT(DISTINCT "PULocationID") 
FROM public.yellow_trip_data;

-------------------------------------------------------------------------------------------------------------------------
-- Question 3.
-- 5.649 s
SELECT "PULocationID" 
FROM public.yellow_trip_data;


-- 4.368 s
SELECT "PULocationID", "DOLocationID"
FROM public.yellow_trip_data;

-------------------------------------------------------------------------------------------------------------------------
-- Question 4.
SELECT COUNT(*)
FROM public.yellow_trip_data
WHERE "fare_amount" = 0;

-------------------------------------------------------------------------------------------------------------------------
-- Question 6.
SELECT DISTINCT "VendorID"
FROM public.yellow_trip_data
WHERE "tpep_dropoff_datetime" BETWEEN '2024-03-01' AND '2024-03-15';