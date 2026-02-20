/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# Set the asset name (recommended: staging.trips).
name: staging.trips
# Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# Declare dependencies so `bruin run ... --downstream` and lineage work.
# Examples:
# depends:
#   - ingestion.trips
#   - ingestion.payment_lookup
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  #strategy: TODO
  # set incremental_key to your event time column (DATE or TIMESTAMP).
  #incremental_key: TODO_SET_INCREMENTAL_KEY
  # choose `date` vs `timestamp` based on the incremental_key type.
  #time_granularity: TODO_SET_GRANULARITY

# Define output columns, mark primary keys, and add a few checks.
columns:
  - name: vendor_id
    type: integer
    description: Vendor identifier
  - name: pickup_datetime
    type: timestamp
    description: Pickup timestamp
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Dropoff timestamp
  - name: pickup_location_id
    type: integer
    description: Pickup location id
  - name: dropoff_location_id
    type: integer
    description: Dropoff location id
  - name: passenger_count
    type: integer
    description: Number of passengers
    checks:
      - name: non_negative
  - name: trip_distance
    type: numeric
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: total_amount
    type: numeric
    description: Total trip amount
    checks:
      - name: non_negative
  - name: payment_type_name
    type: string
    description: Human-friendly payment type from lookup
  - name: extracted_at
    type: timestamp
    description: Extraction timestamp from ingestion
  - name: service_type
    type: string
    description: Source service type (yellow/green/fhv)

# Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_positive
    description: Ensure that the staging window has at least one row
    query: |
      SELECT COUNT(*) > 0 FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 1

@bruin */

-- Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH raw AS (
  SELECT
    vendor_id,
    pickup_datetime::TIMESTAMP AS pickup_datetime,
    dropoff_datetime::TIMESTAMP AS dropoff_datetime,
    store_and_fwd_flag,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    passenger_count::INTEGER AS passenger_count,
    trip_distance::DOUBLE AS trip_distance,
    fare_amount::DOUBLE AS fare_amount,
    extra::DOUBLE AS extra,
    mta_tax::DOUBLE AS mta_tax,
    tip_amount::DOUBLE AS tip_amount,
    tolls_amount::DOUBLE AS tolls_amount,
    ehail_fee::DOUBLE AS ehail_fee,
    improvement_surcharge::DOUBLE AS improvement_surcharge,
    total_amount::DOUBLE AS total_amount,
    i.payment_type AS payment_type_id,
    trip_type::INTEGER AS trip_type,
    congestion_surcharge::DOUBLE AS congestion_surcharge,
    extracted_at::TIMESTAMP AS extracted_at,
    service_type
  FROM ingestion.trips i
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
    AND pickup_datetime IS NOT NULL
    AND total_amount IS NOT NULL
    AND total_amount >= 0
),
joined AS (
  SELECT r.*, p.payment_type_name
  FROM raw r
  LEFT JOIN ingestion.payment_lookup p
    ON r.payment_type_id = p.payment_type_id
),
deduped AS (
  SELECT
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type_name,
    trip_type,
    congestion_surcharge,
    extracted_at,
    service_type,
    ROW_NUMBER() OVER (
      PARTITION BY vendor_id, pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, total_amount
      ORDER BY extracted_at DESC
    ) AS rn
  FROM joined
)
SELECT
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  store_and_fwd_flag,
  rate_code_id,
  pickup_location_id,
  dropoff_location_id,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type_name,
  trip_type,
  congestion_surcharge,
  extracted_at,
  service_type
FROM deduped
WHERE rn = 1

