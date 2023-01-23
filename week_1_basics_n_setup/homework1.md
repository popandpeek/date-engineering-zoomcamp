# Question 1
docker build --help

# Question 2
pip list

# Question 3
SELECT COUNT(*)
FROM green_taxi_trips
WHERE to_char(lpep_pickup_datetime, 'YYYY-MM-DD') = '2019-01-15' AND 
	to_char(lpep_dropoff_datetime, 'YYYY-MM-DD') = '2019-01-15';

# Question 4
SELECT lpep_pickup_datetime
FROM green_taxi_trips
WHERE trip_distance = (SELECT MAX(trip_distance) FROM green_taxi_trips);

#Question 5
SELECT COUNT(*)
FROM green_taxi_trips
WHERE to_char(lpep_pickup_datetime, 'YYYY-MM-DD') = '2019-01-01' AND
	passenger_count = 2;

SELECT COUNT(*)
FROM green_taxi_trips
WHERE to_char(lpep_pickup_datetime, 'YYYY-MM-DD') = '2019-01-01' AND
	passenger_count = 3;

# Question 6
WITH cte AS (
	SELECT *
	FROM green_taxi_trips 
	INNER JOIN zones ON green_taxi_trips."PULocationID" = zones."LocationID"
	WHERE zones."Zone" = 'Astoria'
	ORDER BY tip_amount  DESC
	LIMIT 1
)

SELECT zones."Zone"
FROM cte
INNER JOIN zones ON cte."DOLocationID" = zones."LocationID";