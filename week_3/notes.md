# Important points in GCP BigQuery - Datawarehouse

Make sure we have gcp bucket and bigquery project in the same region

There's some limit on number of partitions we can have on a table - 4000 partitions but we can comeup with expiring partitioning startegy

### External table creation
we can test create external table using external source i.e., buckets


CREATE OR REPLACE EXTERNAL TABLE `datazoomcamp-375017.dezoomcampzurich.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_datazoomcamp-375017/yellow_tripdata_2019-*.csv']
);

then select for testing
select * from `dezoomcampzurich.external_yellow_tripdata` limit 20


### non-partitioned table
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE datazoomcamp-375017.dezoomcampzurich.external_yellow_tripdata_non_partitoned AS
SELECT * FROM `dezoomcampzurich.external_yellow_tripdata`

### partitioned table
-- Create a partitioned table from external table
CREATE OR REPLACE TABLE datazoomcamp-375017.dezoomcampzurich.external_yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `dezoomcampzurich.external_yellow_tripdata`



### partitioned vs non-paritioned table performance - amount of data reading and time - inturns effect costs for query run
-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM datazoomcamp-375017.dezoomcampzurich.external_yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-25' AND '2019-01-30';


SELECT DISTINCT(VendorID)
FROM datazoomcamp-375017.dezoomcampzurich.external_yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-25' AND '2019-01-30';



### check paritions - like metadata of each partition for a particular table
-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `dezoomcampzurich.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'external_yellow_tripdata_non_partitoned'
ORDER BY total_rows DESC;

SELECT table_name, partition_id, total_rows
FROM `dezoomcampzurich.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'external_yellow_tripdata_partitoned'
ORDER BY total_rows DESC;


### partitioned and clustered table
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `datazoomcamp-375017.dezoomcampzurich.external_yellow_tripdata_partitoned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `dezoomcampzurich.external_yellow_tripdata`




### partitioned VS clustered table
SELECT count(*) as trips
FROM `dezoomcampzurich.external_yellow_tripdata_partitoned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-01-24'
  AND VendorID=100000;

SELECT count(*) as trips
FROM `dezoomcampzurich.external_yellow_tripdata_partitoned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-01-01' AND '2019-01-24'
  AND VendorID=100000;

in this example, even though both partitioned and partitioned_clustered queries showing they will scan same 88.92 MB, but the "only partitioned query" returned 0 rows and yet billed for 89MB of data processed and whereas the "partitioned and clustered query" returns 0 rows but billed for 0MB of data processed


----------------------------------------------------------------------------------------------------

## Clustering

1. order is important - specifies the sort order

2. table < 1GB, don't show significance improve with partition and cluster

3. we can specify upto 4 clustering columns, also must be top-level and non-repeated columns

4. few points on when to choose clustering over partitioning

5. BigQuery performs automatic re-clustering in the background to restore the sort order property of the table

6. for partitioned tables, clustering is maintained for data within the scope of each partition

----------------------------------------------------------------------------------------------------

## BigQuery best practices
### Cost
1. avoid select *
2. price your queries before running them
3. use clustered or partitioned tables
4. use streaming inserts with caution
5. materialize query results in stages
### Query
1. filter on partitioned columns
2. denormalizing data
3. use nested or repeated columns
4. use external sources appropriately
5. not for a high query performance
6. reduce data before a join
7. do no treat WITH clause as prepared statements
8. avoid oversharding tables

----------------------------------------------------------------------------------------------------
## Internals of BigQuery

1. follows columnar oriented storage
2. BigQuery separates the storage(Colossus) and compute hardwares thus giving advantage that if the data size increases we just need to pay for the storage addition which is very cheaper compared to the compute hardware
3. However, we might get a question how BigQuery compute communicates/access the storage, wouldn't be there some network delays?
    a. so for this the jupiter network comes into the picture
    b. jupiter network is inside bigquery datacentres
    c. provides 1TB/sec network speed
4. Dremel : query execution engine, tree structure, separates query such that each node can execute individual subset of the query
5. column oriented storage provides better help on aggregations, and also generally on data warehouse we usually query few columns and do filter and aggreations on them
6. Distributions of the workers(to divide query into subquery and aggregate results back) - here is how BigQuery works so fast


