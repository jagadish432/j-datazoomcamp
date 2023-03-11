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


