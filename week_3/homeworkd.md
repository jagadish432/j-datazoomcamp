# week-3 homework setup
## Load data to GCP bucket
we have used the [python file](load_data_to_bucket.py) to load data to gcp bucket only, Later realized we shoudln't do any transformation, so I just uploaded the files manually in gcp console

## create an external table in BQ using fhv 2019 data

CREATE OR REPLACE EXTERNAL TABLE `datazoomcamp-375017.dezoomcampzurich.external_fhv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_datazoomcamp-375017/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

## create a table in BQ using fhv 2019 data


CREATE OR REPLACE TABLE datazoomcamp-375017.dezoomcampzurich.external_fhv_non_partitoned AS
SELECT * FROM `dezoomcampzurich.external_fhv`


## Questions
1. What is the count for fhv vehicle records for year 2019?

Answer => 43244696



2. Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

Answer => 
select count(distinct(Affiliated_base_number)) from `dezoomcampzurich.external_fhv` => shows 0B as estimation

select count(distinct(Affiliated_base_number)) from `dezoomcampzurich.external_fhv_non_partitoned` => shows 317.94MB as estimation


3. How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

Answer => 
select count(*) from `dezoomcampzurich.external_fhv_non_partitoned`
where PUlocationID is null and DOlocationID is null

717748


4. What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

Answer => 
Partition by pickup_datetime Cluster on affiliated_base_number


5. Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

Answer =>

create partitioned and clustered table with below query

CREATE OR REPLACE TABLE `datazoomcamp-375017.dezoomcampzurich.external_fhv_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `dezoomcampzurich.external_fhv`


Now, the below query is to fetch distinct affliated base number between given dates

select distinct(Affiliated_base_number) from `dezoomcampzurich.external_fhv_non_partitoned`
where pickup_datetime between "2019-03-01" and "2019-03-31"  => shows 647.87 MB of data 

and 

select distinct(Affiliated_base_number) from `dezoomcampzurich.external_fhv_partitoned_clustered`
where pickup_datetime between "2019-03-01" and "2019-03-31"  => shows 23.05 MB of data


6. Where is the data stored in the External Table you created?

Answer => GCP Bucket


7. It is best practice in Big Query to always cluster your data:

Answer => False



