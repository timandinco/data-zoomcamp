# Module 7 - Streaming

## Streaming Homework   
------------------------

## Question 1. Redpanda version

Run rpk version inside the Redpanda container:

```
docker exec -it module7-redpanda-1 rpk version
```

What version of Redpanda are you running?

**Answer**
v25.3.9

## Question 2. Sending data to Redpanda

Convert each row to a dictionary and send it to the green-trips topic. You'll need to handle the datetime columns - convert them to strings before serializing to JSON.

Measure the time it takes to send the entire dataset and flush:
```
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```
How long did it take to send the data?

**took 12.40 seconds**


## Question 3. Consumer - trip distance

Write a Kafka consumer that reads all messages from the green-trips topic (set auto_offset_reset='earliest').

Count how many trips have a trip_distance greater than 5.0 kilometers.

How many trips have trip_distance > 5?

```
CREATE TABLE processed_green_events (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    pickup_date VARCHAR,
    dropoff_date VARCHAR,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION
);

----- 

postgres@localhost:postgres> SELECT COUNT(*) FROM processed_green_events WHERE trip_distance > 5
+-------+
| count |
|-------|
| 8506  |
+-------+
SELECT 1
Time: 0.020s
```

## Part 2: PyFlink (Questions 4-6)
For the PyFlink questions, you'll adapt the workshop code to work with the green taxi data. The key differences from the workshop:

- Topic name: green-trips (instead of rides)
- Datetime columns use lpep_ prefix (instead of tpep_)
- You'll need to handle timestamps as strings (not epoch milliseconds)

```SQL
CREATE TABLE processed_green_events (
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    pickup_date VARCHAR,
    dropoff_date VARCHAR,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION
);
```

## Question 4. Tumbling window - pickup location

Create a Flink job that reads from green-trips and uses a 5-minute tumbling window to count trips per PULocationID.

Write the results to a PostgreSQL table with columns: window_start, PULocationID, num_trips.

After the job processes all data, query the results:

```SQL
SELECT PULocationID, num_trips
FROM processed_green_trip_aggregated
ORDER BY num_trips DESC
LIMIT 3;
```

Which PULocationID had the most trips in a single 5-minute window?

**Answer:**

74

Helpful Notes: 
```
docker exec -it module7-redpanda-1 rpk topic create green-trips

docker exec -it module7-redpanda-1 rpk topic list

docker exec -it module7-redpanda-1 rpk version

docker exec -it module7-redpanda-1 rpk topic consume green-trips -n 10

docker exec -it module7-redpanda-1 rpk topic describe green-trips


Start things in the right order
This matters more than people expect:
```
```SQL
CREATE TABLE processed_green_trip_aggregated (
     window_start TIMESTAMP,
     PULocationID INT,
     num_trips BIGINT,
     PRIMARY KEY (window_start, PULocationID)
 );
```
```
Start Kafka/Redpanda
Create topic
Start your Python producer
Start Flink job
``` 

## Question 5. Session window - longest streak
Create another Flink job that uses a session window with a 5-minute gap on PULocationID, using lpep_pickup_datetime as the event time with a 5-second watermark tolerance.

A session window groups events that arrive within 5 minutes of each other. When there's a gap of more than 5 minutes, the window closes.

Write the results to a PostgreSQL table and find the PULocationID with the longest session (most trips in a single session).

How many trips were in the longest session?

```SQL
CREATE TABLE processed_green_trip_sessions (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PULocationID INT,
    num_trips BIGINT,
    PRIMARY KEY (window_start, window_end, PULocationID) 
);
```


The key to this task is the **PARTITION BY** statement. Without this, the num_trips inculdes all PULocationID's for a window. See -> https://nightlies.apache.org/flink/flink-docs-master/docs/sql/reference/queries/window-tvf/#session

```SQL

INSERT INTO {aggregated_table}
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(TABLE {source_table} PARTITION BY PULocationID, 
                    DESCRIPTOR(event_timestamp), 
                    INTERVAL '5' MINUTE)
        )
        GROUP BY PULocationID, window_start, window_end;
```

```
docker exec -it module7-jobmanager-1 flink run -py /opt/src/job/green_trip_job_session.py
```

**Answer**

81

## Question 6. Tumbling window - largest tip

Create a Flink job that uses a 1-hour tumbling window to compute the total tip_amount per hour (across all locations).

Which hour had the highest total tip amount?

```
docker exec -it module7-jobmanager-1 flink run -py /opt/src/job/green_trip_job_tips.py
```


**Answer**

2025-10-16 18:00:00



