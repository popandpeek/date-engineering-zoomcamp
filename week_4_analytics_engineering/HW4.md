## Question 1: What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?

## Question 2: What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?

Green:
https://github.com/popandpeek/date-engineering-zoomcamp/blob/main/week_4_analytics_engineering/taxi_rides_ny/models/staging/stg_green_tripdata.sql

Yellow:
https://github.com/popandpeek/date-engineering-zoomcamp/blob/main/week_4_analytics_engineering/taxi_rides_ny/models/staging/stg_yellow_tripdata.sql

fact_trips:
https://github.com/popandpeek/date-engineering-zoomcamp/blob/main/week_4_analytics_engineering/taxi_rides_ny/models/core/fact_trips.sql

https://lookerstudio.google.com/s/k94qZbg4LxU

## Question 3: What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?

## Question 4: What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?

## Question 5: What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?

fhv:
https://github.com/popandpeek/date-engineering-zoomcamp/blob/main/week_4_analytics_engineering/taxi_rides_ny/models/staging/stg_fhv_tripdata.sql

fhv_fact_trips:
https://github.com/popandpeek/date-engineering-zoomcamp/blob/main/week_4_analytics_engineering/taxi_rides_ny/models/core/fact_trips_fhv.sql

https://lookerstudio.google.com/s/v3VhjK-OiaY