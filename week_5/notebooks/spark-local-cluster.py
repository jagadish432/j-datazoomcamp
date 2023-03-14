#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()
input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder.master("spark://de-zoomcap.asia-south1-c.c.datazoomcamp-375017.internal:7077").appName('local_cluster').getOrCreate()


df_green = spark.read.parquet(input_green)
df_yellow = spark.read.parquet(input_yellow) 



df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime').withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime').withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


set(df_green.columns) & set(df_yellow.columns)
set(df_green.columns) - set(df_yellow.columns)

common_cols = []

for col in df_yellow.columns:
    if col in df_green.columns:
        common_cols.append(col)

common_cols


df_green_sel = df_green.select(common_cols).withColumn('service_type', F.lit('green'))
df_yellow_sel = df_yellow.select(common_cols).withColumn('service_type', F.lit('yellow'))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)
df_trips_data.groupBy('service_type').count().show()
df_trips_data.registerTempTable('trips_data')

df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    --Note: For BQ use instead: date_trunc(pickup_datetime, month) AS revenue_month, 

    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(pASsenger_count) AS AVG_montly_pASsenger_count,
    AVG(trip_distance) AS AVG_montly_trip_distance

FROM 
    trips_data
GROUP BY 1,2,3
""")


df_result.coalesce(1).write.parquet(output, mode='overwrite')
