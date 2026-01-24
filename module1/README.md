# Module 1 Homework

## Question 1. Understanding Docker images

What is the version of `pip` in the docker image `python:3.13`?

**Notes**

Basic interactive shell:\
`docker run --rm -it --entrypoint /bin/bash python:3.13`

With workspace mounted (runs bash with current directory mounted at /workspace):\
`docker run --rm -it --entrypoint /bin/bash -v "$(pwd)":/workspace -w /workspace python:3.13`

**Answer**: 25.3

## Question 2. Understanding Docker networking and docker-compose
Understanding Docker networking and docker-compose

```
Your Laptop
└── localhost:5433
    └── Docker bridge
        └── postgres container:5432
            └── db
            └── postgres

pgAdmin container
└── talks directly to db:5432
```
**Answer(s)**:
- postgres:5432
- db:5432


## Question 3. Counting short trips
For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?

```sql
SELECT COUNT(*) 
FROM green_taxi_trips
WHERE trip_distance <= 1
	AND EXTRACT(Month from lpep_pickup_datetime) = 11

SELECT COUNT(*) 
FROM green_taxi_trips
WHERE trip_distance <= 1
	AND CAST(lpep_pickup_datetime as DATE) >= '2025-11-01' 
	AND CAST(lpep_pickup_datetime as DATE) < '2025-12-01' 

SELECT COUNT(*) 
FROM green_taxi_trips
WHERE trip_distance <= 1
	AND lpep_pickup_datetime >= '2025-11-01' 
	AND lpep_pickup_datetime < '2025-12-01' 
```
**Answer**: 8007

## Question 4. Longest trip for each day
Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).

```sql
SELECT lpep_pickup_datetime, lpep_dropoff_datetime, trip_distance
FROM green_taxi_trips 
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 50

SELECT lpep_pickup_datetime, trip_distance
FROM green_taxi_trips 
WHERE trip_distance = (
	SELECT MAX(trip_distance)
	FROM green_taxi_trips 
	WHERE trip_distance < 100)

-- "2025-11-14 15:36:27"	88.03
```

**Answer**: 2025-11-14

## Question 5. Biggest pickup zone
Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

```sql
SELECT COUNT(*) AS tally
	, CAST(lpep_pickup_datetime AS DATE)
	, "PULocationID"
FROM green_taxi_trips
WHERE CAST(lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY CAST(lpep_pickup_datetime AS DATE)
	, "PULocationID"
ORDER BY tally DESC

-- 74,"Manhattan","East Harlem North","Boro Zone"
```
**Answer**: East Harlem North

## Question 6. Largest tip
For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

```sql
SELECT * 
FROM green_taxi_trips
WHERE "PULocationID" = 74
	AND EXTRACT(MONTH from lpep_pickup_datetime) = 11
	AND EXTRACT(YEAR from lpep_pickup_datetime) = 2025
ORDER BY tip_amount DESC


SELECT "DOLocationID", tip_amount
FROM green_taxi_trips
WHERE tip_amount = (
SELECT MAX(tip_amount)
FROM green_taxi_trips
WHERE "PULocationID" = 74
	AND EXTRACT(MONTH from lpep_pickup_datetime) = 11
	AND EXTRACT(YEAR from lpep_pickup_datetime) = 2025)

-- 263,"Manhattan","Yorkville West","Yellow Zone"   

```

**Answer**: Yorkville West