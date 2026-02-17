SELECT
  -- identifiers
  CAST(dispatching_base_num AS varchar) as dispatching_base_number,
  CAST(PUlocationID as int) as pickup_location_id,
  CAST(DOlocationID as int) as dropoff_location_id,

  -- timestamps
  CAST(pickup_datetime as timestamp) as pickup_datetime,
  CAST(dropOff_datetime as timestamp) as dropoff_datetime,

  -- flags / misc
  SR_Flag as sr_flag,
  Affiliated_base_number as affiliated_base_number
FROM {{ source('raw_data', 'fhv_tripdata') }}
WHERE dispatching_base_num IS NOT NULL



