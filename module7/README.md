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

## Question 4. Tumbling window - pickup location






