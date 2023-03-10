## Batch Processing

1. Batch processing is most used in companies when compared to the streaming processing
2. Daily, Hourly, Monthly, thrice a hour, etc..
3. Delay is there before we get proceesed data but that delay is expected and acceptable, in case of very urgently needed usecase we should switch to streming processing only
4.  SQL, Python, Spark, Flink, etc..

### Spark
1. Data processing engine
2. handles large scale data processing
3. Data lake to Data lake/Data warehouse
4. Java, Scala, Python-PySpark, R
5. Spark can handle both batch and stream data
6. To process data from DL(eg: S3/GCS with parquet/csv files) and store into DL/DwH
    a. we can use SQL (HIVE ,PRESTO, ATHENA)
    b. we can use SPARK
7. If we can express our job as SQL, then go with it
8. sometimes we may face complex scenarios, or we may need unittests and modularity, or we may not find a way to express them in SQL then we can use SPARK

### Sample workflows
1. Raw data -> Lake -> SQL/ATHENA -> SPARK -> Python Train ML

### Install Spark
1. for windows - https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/windows.md
2. for linux - https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/linux.md
3. for macos - https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/setup/macos.md



### Actions vs Transformations
#### Transformations:
are Lazy(not executed immediately)
1. Selecting columns
2. Filtering
3. Joins
4. Groupby


#### Actions:
are Eager
1. Show, take, head
2. Write


### SPARK internals
#### spark cluster
0. spark driver is where we run the spark to submit our job/task
1. when we submit a job to spark cluster, the master gets the job and assigns sub-tasks/sub-jobs to its executors
2. each executor would pull the required data from the dataframe(df)
3. this df could reside in AWs S3/GCP bucket/Hadoop HDFS/etc..
4. the DF would have internal prtitions, although the each partition would be some individual parquet file
5. each executor will pull the respective partition from the df 
6. the idea with HDFS is instead of pulling data into machine, it'll pull the code into the machine which has the data. because code size is always << than the data size
7. But these days with the addition of cloud providers, the bucket and the machine would be in the same data centers so downloading 100mb data is nothing but some milliseconds matter
8. once each executor get their results, spark re-shuffle the outputs by like parition based on the result set , it resuffle the resut sets using external merge sort


#### Joins
1. in case of joining large table with a small table, the small table gets broadcasted to every executor so that we dont need to reshuffle, it's much faster


### spark-gcloud connection
0. make sure you logged in already using `gcloud auth login`
1. upload all parquet files form gcp compute machine(local) to the GCS bucket ` gsutil -m cp -r pq/ gs://dtc_data_lake_datazoomcamp-375017/pq`. -m is for multithread/processing, -r is for recursive, copying from local pq/ folder to the GCS bucket new `pq` folder
2. copy the gcloud connector hadoop library from gcloud onto the local directory `gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar lib/gcs-connector-hadoop3-2.2.5.jar` in order for us to be able to load data from files stored in the GCS bucket
3. to kill the spark context "kill `lsof -t -i:4040`"
4. 
