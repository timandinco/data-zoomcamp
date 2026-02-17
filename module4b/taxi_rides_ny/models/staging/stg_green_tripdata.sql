
SELECT 
  -- identifiers
  CAST(VendorID as int) as vendor_id,
  CAST(RatecodeID as int) as rate_code_id,
  CAST(PULocationID as int) as pickup_location_id,
  CAST(DOLocationID as int) as dropoff_location_id,

  -- timestamps
  CAST(lpep_pickup_datetime as timestamp) as pickup_datetime,
  CAST(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

  -- trip details
  store_and_fwd_flag,
  CAST(passenger_count as int) as passenger_count,
  CAST(trip_distance as float) as trip_distance,
  CAST(trip_type as int) as trip_type,

  -- payment details
  CAST(fare_amount as decimal(18,3)) as fare_amount,
  CAST(extra as decimal(18,3)) as extra,
  CAST(mta_tax as decimal(18,3)) as mta_tax,
  CAST(tip_amount as decimal(18,3)) as tip_amount,
  CAST(tolls_amount as decimal(18,3)) as tolls_amount,
  CAST(ehail_fee as decimal(18,3)) as ehail_fee,
  CAST(improvement_surcharge as decimal(18,3)) as improvement_surcharge,
  CAST(total_amount as decimal(18,3)) as total_amount,
  CAST(payment_type as int) as payment_type,
  congestion_surcharge
FROM {{ source('raw_data', 'green_tripdata') }}
WHERE VendorId IS NOT NULL
  AND lpep_pickup_datetime IS NOT NULL
  AND lpep_dropoff_datetime IS NOT NULL
