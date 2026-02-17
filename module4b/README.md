# Module 4 Homework

## Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run dbt run --select int_trips_unioned, what models will be built?

**LLM Notes**

Given this dependency graph:

```
stg_green_tripdata   stg_yellow_tripdata
          \              /
           \            /
            int_trips_unioned
```

And you run:

```bash
dbt run --select int_trips_unioned
```

---

# 🎯 What Gets Built?

✅ **Only** `int_trips_unioned`

❌ `stg_green_tripdata` — NOT built
❌ `stg_yellow_tripdata` — NOT built

---

# 🧠 Why?

By default, `--select` includes **only the specified node**, not its parents or children.

So dbt interprets:

> "Build exactly this model."

It does **not** automatically build upstream dependencies unless you tell it to.

---

# 🚀 If You Want Upstream Models Built Too

Use the `+` selector:

### Build model + its parents:

```bash
dbt run --select +int_trips_unioned
```

That builds:

* stg_green_tripdata
* stg_yellow_tripdata
* int_trips_unioned

---

### Build model + parents + children:

```bash
dbt run --select +int_trips_unioned+
```

---

# 🎯 Important Real-World Detail

Even though dbt doesn’t rebuild the staging models, it will still:

* Reference them via `ref()`
* Expect their tables/views to already exist

So if you haven’t built them yet, the run will fail.

---

# 📌 Quick Selector Cheat Sheet

| Command                        | What Gets Built               |
| ------------------------------ | ----------------------------- |
| `--select int_trips_unioned`   | Only that model               |
| `--select +int_trips_unioned`  | Model + upstream dependencies |
| `--select int_trips_unioned+`  | Model + downstream children   |
| `--select +int_trips_unioned+` | Entire subgraph               |

---

# 🧠 Mental Model

Think of:

* `+` on the left → parents (upstream)
* `+` on the right → children (downstream)

---

Perfect — this *is* a common trick question in interviews and in the Zoomcamp exam.

Let’s break it down clearly.

---

# 🧠 First: What’s the Difference?

## `dbt run`

* Builds **models only**
* Does **not** run tests
* Does **not** run seeds or snapshots

## `dbt build`

* Builds models
* Runs tests
* Runs seeds
* Runs snapshots
* In dependency order

Think of:

> `dbt build = run + test + seed + snapshot`

---

# 🎯 Now Using Your Example Graph

```
stg_green_tripdata   stg_yellow_tripdata
          \              /
           \            /
            int_trips_unioned
```

---

# Scenario 1

```bash
dbt run --select int_trips_unioned
```

Builds:

* ✅ int_trips_unioned
* ❌ NOT staging models
* ❌ NOT tests

Assumes parents already exist.

---

# Scenario 2

```bash
dbt build --select int_trips_unioned
```

Builds:

* ✅ int_trips_unioned
* ❌ NOT staging models
* ✅ Runs tests for int_trips_unioned only

Important: It does **not** automatically build parents unless you add `+`.

---

# Scenario 3 (Most Common Production Pattern)

```bash
dbt build --select +int_trips_unioned
```

Builds:

* ✅ stg_green_tripdata
* ✅ stg_yellow_tripdata
* ✅ int_trips_unioned
* ✅ Tests for all three models

Because `+` pulls in upstream dependencies.

---

# 🚨 Important Subtle Difference

Here’s the trick:

### `dbt run` ignores tests entirely.

### `dbt build` respects dependency graph for tests.

Example:

If you do:

```bash
dbt build --select int_trips_unioned
```

It will:

* Build `int_trips_unioned`
* Run **its tests**
* But will NOT run staging model tests (unless selected)

---

# 🧠 Even More Subtle

If you do:

```bash
dbt build --select +int_trips_unioned
```

dbt will:

* Build upstream models
* Build int model
* Run tests for all of them

This is why `build` is preferred in CI/CD pipelines.

---

# 🎓 Interview Trick Question

> If you run `dbt build --select model_x`, will upstream tests run?

Answer:

No.
Only tests for selected nodes are included unless `+` expands selection.

---

# 🚀 Best Practice Patterns

### Development (fast iteration)

```bash
dbt run --select my_model
```

### Rebuilding a subgraph

```bash
dbt build --select +my_model
```

### CI / Production

```bash
dbt build
```

(full DAG, all tests)

---

# 🎯 Mental Model

| Command      | Builds Parents? | Runs Tests? |
| ------------ | --------------- | ----------- |
| run          | ❌               | ❌           |
| run +model   | ✅               | ❌           |
| build        | ❌               | ✅           |
| build +model | ✅               | ✅           |

---

# 🔥 Why This Matters

In production:

* `run` can leave bad data undetected
* `build` ensures data quality checks run
* `+` ensures DAG consistency

---

## Question 2. - dbt Tests

You've configured a generic test like this in your schema.yml:

columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
Your model fct_trips has been running successfully for months. A new value 6 now appears in the source data.

What happens when you run dbt test --select fct_trips?

```
00:35:34  Finished running 9 data tests in 0 hours 0 minutes and 39.44 seconds (39.44s).
00:35:35  
00:35:35  Completed with 1 error, 0 partial successes, and 0 warnings:
00:35:35  
00:35:35  Failure in test accepted_values_fct_trips_service_type__Green (models/marts/schema.yml)
00:35:35    Got 1 result, configured to fail if != 0
00:35:35  
00:35:35    compiled code at target/compiled/taxi_rides_ny/models/marts/schema.yml/accepted_values_fct_trips_service_type__Green.sql
00:35:35  
00:35:35  Done. PASS=8 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=9
```

## Question 3. Counting Records in fct_monthly_zone_revenue

After running your dbt project, query the fct_monthly_zone_revenue model.

What is the count of records in the fct_monthly_zone_revenue model?

```sql
SELECT COUNT(*) FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
-- 12184
```

## Question 4. Best Performing Zone for Green Taxis (2020)

Using the fct_monthly_zone_revenue table, find the pickup zone with the highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020.

Which zone had the highest revenue?

```sql
SELECT * 
FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
WHERE service_type = 'Green'
AND date_part('year', revenue_month) = 2020
ORDER BY revenue_monthly_total_amount DESC
LIMIT 20

```
**Answer**
East Harlem North

## Question 5. Green Taxi Trip Counts (October 2019)

Using the fct_monthly_zone_revenue table, what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?

```sql
SELECT SUM(total_monthly_trips) as sum_total_monthly_trips
FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
WHERE service_type = 'Green'
AND date_part('year', revenue_month) = 2019
AND date_part('month', revenue_month) = 10
-- 384624
```

## Question 6. Build a Staging Model for FHV Data

Create a staging model for the For-Hire Vehicle (FHV) trip data for 2019.

  1. Load the FHV trip data for 2019 into your data warehouse
  2. Create a staging model stg_fhv_tripdata with these requirements:
     - Filter out records where dispatching_base_num IS NULL
     - Rename fields to match your project's naming conventions (e.g., PUlocationID → pickup_location_id)

What is the count of records in stg_fhv_tripdata?