#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://de-zoomcap.asia-south1-c.c.datazoomcamp-375017.internal:7077").appName('local_cluster').getOrCreate()


# In[2]:


spark


# In[3]:


df_green = spark.read.parquet('./data/pq/green/*/*')


# In[4]:


df_yellow = spark.read.parquet('data/pq/yellow/*/*') 


# In[5]:


df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')                 .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')                 .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


# In[6]:


set(df_green.columns) & set(df_yellow.columns)


# In[7]:


set(df_green.columns) - set(df_yellow.columns)


# In[8]:


common_cols = []

for col in df_yellow.columns:
    if col in df_green.columns:
        common_cols.append(col)


# In[9]:


common_cols


# In[10]:


from pyspark.sql import functions as F


# In[11]:


df_green_sel = df_green.select(common_cols)     .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow.select(common_cols)     .withColumn('service_type', F.lit('yellow'))


# In[13]:


df_trips_data = df_green_sel.unionAll(df_yellow_sel)


# In[14]:


df_trips_data.groupBy('service_type').count().show()


# In[15]:


df_trips_data.registerTempTable('trips_data')


# In[16]:


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


# In[ ]:


df_result.coalesce(1).write.parquet('data/report/revenue', mode='overwrite')

