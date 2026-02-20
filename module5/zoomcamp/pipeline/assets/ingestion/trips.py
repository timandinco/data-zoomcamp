"""@bruin

# Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# Set the connection.
connection: duckdb-zoomcamp

# Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # choose `table` or `view` (ingestion generally should be a table)
  type: table
  # pick a strategy.
  # suggested strategy: append
  strategy: append

# Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: vendor_id
    type: integer
    description: Vendor identifier (raw `VendorID`).
  - name: pickup_datetime
    type: timestamp
    description: Pickup datetime (raw `lpep_pickup_datetime` / `tpep_pickup_datetime`).
  - name: dropoff_datetime
    type: timestamp
    description: Dropoff datetime (raw `lpep_dropoff_datetime` / `tpep_dropoff_datetime`).
  - name: store_and_fwd_flag
    type: string
    description: Store-and-forward flag.
  - name: rate_code_id
    type: integer
    description: Rate code id (raw `RatecodeID`).
  - name: pickup_location_id
    type: integer
    description: Pickup location id (raw `PULocationID`).
  - name: dropoff_location_id
    type: integer
    description: Dropoff location id (raw `DOLocationID`).
  - name: passenger_count
    type: integer
    description: Number of passengers.
  - name: trip_distance
    type: numeric
    description: Trip distance in miles.
  - name: fare_amount
    type: numeric
    description: Fare amount.
  - name: extra
    type: numeric
    description: Extra charges.
  - name: mta_tax
    type: numeric
    description: MTA tax.
  - name: tip_amount
    type: numeric
    description: Tip amount.
  - name: tolls_amount
    type: numeric
    description: Tolls amount.
  - name: ehail_fee
    type: numeric
    description: e-hail fee (if present).
  - name: improvement_surcharge
    type: numeric
    description: Improvement surcharge.
  - name: total_amount
    type: numeric
    description: Total trip amount.
  - name: payment_type
    type: integer
    description: Payment type code.
  - name: trip_type
    type: integer
    description: Trip type code.
  - name: congestion_surcharge
    type: numeric
    description: Congestion surcharge.
  - name: extracted_at
    type: timestamp
    description: Extraction timestamp added by the ingestion asset.
  - name: service_type
    type: string
    description: Taxi service type (e.g., `yellow`, `green`, `fhv`).

@bruin"""

import os
import json
from datetime import datetime, date
from pathlib import Path
import requests
import duckdb
import pandas as pd


def _months_between(start_date: date, end_date: date):
  cur = date(start_date.year, start_date.month, 1)
  end = date(end_date.year, end_date.month, 1)
  months = []
  while cur <= end:
    months.append((cur.year, cur.month))
    if cur.month == 12:
      cur = date(cur.year + 1, 1, 1)
    else:
      cur = date(cur.year, cur.month + 1, 1)
  return months


def materialize():
  """Ingest monthly parquet files from the TLC public feed and return a DataFrame.

  Behavior:
  - Reads pipeline variables from `BRUIN_VARS` (JSON); expects `taxi_types` list.
  - Reads date window from `BRUIN_START_DATE` and `BRUIN_END_DATE` (YYYY-MM-DD).
  - Downloads monthly parquet files from the public CloudFront endpoint and caches them under `data/{taxi_type}`.
  - Loads each parquet with DuckDB into a pandas DataFrame and concatenates results.
  - Adds `extracted_at` column with current UTC timestamp.

  Returns:
    pandas.DataFrame: concatenated rows for the requested taxi types & months.
  """
  # Config / defaults
  vars_json = os.getenv("BRUIN_VARS")
  if vars_json:
    try:
      vars_obj = json.loads(vars_json)
    except Exception:
      vars_obj = {}
  else:
    vars_obj = {}

  taxi_types = vars_obj.get("taxi_types", ["yellow"]) if vars_obj is not None else ["yellow"]

  start_str = os.getenv("BRUIN_START_DATE")
  end_str = os.getenv("BRUIN_END_DATE")
  if start_str and end_str:
    start_date = datetime.fromisoformat(start_str).date()
    end_date = datetime.fromisoformat(end_str).date()
  else:
    # sensible default: last month
    today = datetime.utcnow().date()
    first_of_this_month = date(today.year, today.month, 1)
    # set to previous month
    if first_of_this_month.month == 1:
      start_date = date(first_of_this_month.year - 1, 12, 1)
    else:
      start_date = date(first_of_this_month.year, first_of_this_month.month - 1, 1)
    end_date = start_date

  months = _months_between(start_date, end_date)

  BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
  out_root = Path("data")
  out_root.mkdir(exist_ok=True)

  dfs = []
  extracted_at = datetime.utcnow().isoformat()

  for taxi_type in taxi_types:
    data_dir = out_root / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year, month in months:
      filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
      filepath = data_dir / filename
      url = f"{BASE_URL}/{filename}"

      if not filepath.exists():
        print(f"Downloading {url} -> {filepath}")
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        with open(filepath, "wb") as f:
          for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
              f.write(chunk)
      else:
        print(f"Using cached file {filepath}")

      # Load parquet into pandas via DuckDB for robust parquet handling
      con = duckdb.connect(database=':memory:')
      try:
        df = con.execute(f"SELECT * FROM parquet_scan('{filepath}')").fetchdf()
      finally:
        con.close()

      if not df.empty:
        df["extracted_at"] = extracted_at
        df["service_type"] = taxi_type
        dfs.append(df)

  if dfs:
    result = pd.concat(dfs, ignore_index=True)
  else:
    result = pd.DataFrame()

  # Normalize source column names to the canonical schema expected by downstream assets
  if not result.empty:
    # map of canonical_name -> candidate source names (checked case-insensitively)
    candidates = {
      'vendor_id': ['vendorid', 'vendor_id'],
      'pickup_datetime': ['lpep_pickup_datetime', 'tpep_pickup_datetime', 'pickup_datetime'],
      'dropoff_datetime': ['lpep_dropoff_datetime', 'tpep_dropoff_datetime', 'dropoff_datetime'],
      'store_and_fwd_flag': ['store_and_fwd_flag', 'store_and_fwd'],
      'rate_code_id': ['ratecodeid', 'rate_code_id', 'ratecode_id'],
      'pickup_location_id': ['pulocationid', 'pu_location_id', 'pickup_location_id'],
      'dropoff_location_id': ['dolocationid', 'do_location_id', 'dropoff_location_id'],
      'passenger_count': ['passengercount', 'passenger_count'],
      'trip_distance': ['trip_distance', 'tripdistance'],
      'fare_amount': ['fare_amount', 'fareamount'],
      'extra': ['extra'],
      'mta_tax': ['mta_tax', 'mtatax'],
      'tip_amount': ['tip_amount', 'tipamount'],
      'tolls_amount': ['tolls_amount', 'tollsamount'],
      'ehail_fee': ['ehail_fee'],
      'improvement_surcharge': ['improvement_surcharge'],
      'total_amount': ['total_amount', 'total_amounts'],
      'payment_type': ['payment_type', 'payment_type_id', 'paymenttype', 'paymenttypeid'],
      'trip_type': ['trip_type', 'triptype'],
      'congestion_surcharge': ['congestion_surcharge'],
    }

    # create a mapping from lower-case source col -> actual col name
    cols_lower = {c.lower(): c for c in result.columns}
    rename_map = {}
    for canonical, cand_list in candidates.items():
      for cand in cand_list:
        if cand.lower() in cols_lower:
          rename_map[cols_lower[cand.lower()]] = canonical
          break

    if rename_map:
      result = result.rename(columns=rename_map)

  return result


