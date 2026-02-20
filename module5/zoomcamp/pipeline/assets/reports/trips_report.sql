/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  #strategy: TODO
  # set to your report's date column
  #incremental_key: TODO
  # set to `date` or `timestamp`
  #time_granularity: TODO
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

# Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: report_date
    type: date
    description: Aggregation date (pickup date)
    primary_key: true
  - name: service_type
    type: string
    description: Taxi service type (yellow/green/fhv)
    primary_key: true
  - name: trips_count
    type: integer
    description: Number of trips in the window
  - name: avg_trip_distance
    type: numeric
    description: Average trip distance
  - name: avg_total_amount
    type: numeric
    description: Average total amount
  - name: avg_tip_amount
    type: numeric
    description: Average tip amount
  - name: total_revenue
    type: numeric
    description: Sum of total_amount
  - name: avg_passenger_count
    type: numeric
    description: Average passenger count

custom_checks:
  - name: row_count_positive
    description: Ensure the report has at least one aggregated row in the window
    query: |
      SELECT COUNT(*) > 0 FROM reports.trips_report
      WHERE report_date >= '{{ start_datetime }}'::DATE
        AND report_date <= ('{{ end_datetime }}'::TIMESTAMP)::DATE
    value: 1


@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  CAST(date_trunc('day', pickup_datetime) AS DATE) AS report_date,
  service_type,
  COUNT(*) AS trips_count,
  AVG(trip_distance) AS avg_trip_distance,
  AVG(total_amount) AS avg_total_amount,
  AVG(tip_amount) AS avg_tip_amount,
  SUM(total_amount) AS total_revenue,
  AVG(passenger_count) AS avg_passenger_count
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY report_date, service_type
ORDER BY report_date, service_type
