# Module 3 Homework

## Question 1. Counting records

What is count of records for the 2024 Yellow Taxi Data?

```SQL
CREATE OR REPLACE EXTERNAL TABLE `omega-healer-485222-s1.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-jellystar-demo/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE omega-healer-485222-s1.zoomcamp.yellow_tripdata_nonpartitioned_table AS
SELECT * FROM omega-healer-485222-s1.zoomcamp.external_yellow_tripdata;

SELECT COUNT(*) FROM omega-healer-485222-s1.zoomcamp.yellow_tripdata_nonpartitioned_table;
-- 20332093
```

## Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```SQL
SELECT DISTINCT COUNT(PULocationID) FROM omega-healer-485222-s1.zoomcamp.yellow_tripdata_nonpartitioned_table;
-- 155.12 MB

SELECT DISTINCT COUNT(PULocationID) FROM omega-healer-485222-s1.zoomcamp.external_yellow_tripdata;
-- 0 MB
```

## Question 3. Understanding columnar storage

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?

```SQL
SELECT PULocationId 
FROM omega-healer-485222-s1.zoomcamp.yellow_tripdata_nonpartitioned_table;
-- 155.12 MB

SELECT PULocationId, DOLocationID
FROM omega-healer-485222-s1.zoomcamp.yellow_tripdata_nonpartitioned_table;
-- 310.24 MB
```

**LLM**\

In BigQuery every column is stored separately on disk (Google’s proprietary “Capacitor” column-format).
When the SQL planner builds a query it:

Determines which physical data blocks have to be read (all blocks, in your case, because there is no WHERE clause), and
Adds up the compressed size on disk of just the columns that appear in the SELECT list, in JOIN / WHERE / GROUP BY / ORDER BY clauses, or that are needed internally (e.g. partition-time column).
Therefore:

• Query 1 – you reference only the column PULocationId.
BigQuery scans every block that contains that single column.
Compressed size of PULocationId in the table ≈ 155.12 MB, so the “bytes processed” estimate is 155.12 MB.

• Query 2 – you reference PULocationId and DOLocationID.
The engine now has to read two separate column files.
Each column is ~155.12 MB compressed, so 2 × 155.12 MB = 310.24 MB.

That is why the estimate doubles: in a columnar system the cost is roughly proportional to the total size of the individual columns that have to be read, not to the number of rows returned. If you added a third column of similar size you would see ~465 MB, and so on.

A few extra points:

• No predicate, no partition pruning – because you didn’t filter on a partition key or use a WHERE clause, BigQuery can’t skip any row blocks.
• Charges mirror the estimate – you pay for the bytes scanned, so selecting fewer columns (or using filters that let BigQuery skip blocks) saves money.
• Internal vs. external tables – the behaviour is the same: BigQuery still reads only the referenced columns, but with external tables the scan size is based on the source files instead of Capacitor storage.


## Question 4. Counting zero fare trips

How many records have a fare_amount of 0?

```SQL
SELECT COUNT(*)
FROM omega-healer-485222-s1.zoomcamp.yellow_tripdata_nonpartitioned_table 
WHERE fare_amount = 0;
-- 8333

SELECT COUNT(*)
FROM omega-healer-485222-s1.zoomcamp.external_yellow_tripdata 
WHERE fare_amount = 0;
-- 8333

```

## Question 5. Partitioning and clustering

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

```SQL
--Question 5
CREATE OR REPLACE TABLE omega-healer-485222-s1.zoomcamp.yellow_tripdata_partitioned_clustered
PARTITION BY
  DATE(tpep_dropoff_datetime)
  CLUSTER BY VendorId AS
SELECT * FROM omega-healer-485222-s1.zoomcamp.external_yellow_tripdata;
```

## Question 6. Partition benefits
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

```SQL 

SELECT DISTINCT VendorId 
FROM omega-healer-485222-s1.zoomcamp.yellow_tripdata_partitioned_clustered
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
--26.84 MB

SELECT DISTINCT VendorId 
FROM omega-healer-485222-s1.zoomcamp.yellow_tripdata_nonpartitioned_table
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- 310.24 MB
```

## Question 7. External table storage
Where is the data stored in the External Table you created?

**GCP Bucket**

## Question 8. Clustering best practices
It is best practice in Big Query to always cluster your data:

**True-ish**

## Question 9. Understanding table scans

No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```SQL
SELECT COUNT(*)
FROM omega-healer-485222-s1.zoomcamp.yellow_tripdata_nonpartitioned_table
```
**LLM**\

Because BigQuery does not have to read any of the table’s column data to answer that particular question.
The engine can return an exact row-count by reading only the table’s stored metadata, so the amount of “bytes processed” is reported—and billed—as 0 MB.

What is happening under the hood

BigQuery keeps per-table and per-partition statistics in its internal metadata (row count, column min/max, etc.).

A query of the form

SELECT COUNT(*)
FROM project.dataset.table -- no WHERE, no JOIN, no sub-query, no DISTINCT

can be satisfied entirely from that metadata.

Because no user data blocks are touched, the storage read = 0 bytes.

On-demand pricing is based on bytes read from storage, so the query is free.

When COUNT(*) will NOT be metadata-only

• You add any filter, e.g. WHERE trip_distance > 0
• You reference individual columns, use DISTINCT, GROUP BY, joins, etc.
• You run it against an external table (GCS files, Drive, etc.)—external sources have no BigQuery metadata.
• The table still has rows in the streaming buffer; those rows aren’t yet in the permanent statistics, so BigQuery scans data blocks to give an accurate count.

Other examples of metadata-only queries

• SELECT 1; -- literal query
• SELECT MIN(_PARTITIONTIME), MAX(_PARTITIONTIME) FROM table;
• Queries that read only INFORMATION_SCHEMA views.

So in your case BigQuery can answer the question “how many rows are there?” without opening any of the columnar files, and the UI correctly shows 0 MB processed.


