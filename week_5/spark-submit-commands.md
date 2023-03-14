// using the REST API
{
  "reference": {
    "jobId": "job-63eb98c6",
    "projectId": "datazoomcamp-375017"
  },
  "placement": {
    "clusterName": "dezoomcamp-cluster"
  },
  "status": {
    "state": "DONE",
    "stateStartTime": "2023-03-14T17:34:17.707519Z"
  },
  "yarnApplications": [
    {
      "name": "local_cluster",
      "state": "FINISHED",
      "progress": 1,
      "trackingUrl": "http://dezoomcamp-cluster-m:8088/proxy/application_1678814187027_0001/"
    }
  ],
  "statusHistory": [
    {
      "state": "PENDING",
      "stateStartTime": "2023-03-14T17:33:36.181794Z"
    },
    {
      "state": "SETUP_DONE",
      "stateStartTime": "2023-03-14T17:33:36.279070Z"
    },
    {
      "state": "RUNNING",
      "details": "Agent reported job success",
      "stateStartTime": "2023-03-14T17:33:36.877872Z"
    }
  ],
  "driverControlFilesUri": "gs://dataproc-staging-europe-west6-674561295851-mzb7oila/google-cloud-dataproc-metainfo/2c55393b-50c2-4ba9-989f-2307efa4dc10/jobs/job-63eb98c6/",
  "driverOutputResourceUri": "gs://dataproc-staging-europe-west6-674561295851-mzb7oila/google-cloud-dataproc-metainfo/2c55393b-50c2-4ba9-989f-2307efa4dc10/jobs/job-63eb98c6/driveroutput",
  "jobUuid": "59f4ae3e-b3bd-4326-902a-88763b96e893",
  "done": true,
  "pysparkJob": {
    "mainPythonFileUri": "gs://dtc_data_lake_datazoomcamp-375017/code/spark-dataproc-cluster.py",
    "args": [
      "--input_green=gs://dtc_data_lake_datazoomcamp-375017/pq/green/2021/*",
      "--input_yellow=gs://dtc_data_lake_datazoomcamp-375017/pq/yellow/2021/*",
      "--output=gs://dtc_data_lake_datazoomcamp-375017/report/revenue-2021"
    ]
  }
}


// using gcloud cli , this is command to run python spark file to save results to bucket only
// ensure you have the code file already copied to the gcs location
gcloud dataproc jobs submit pyspark \
    --cluster=dezoomcamp-cluster \
    --region=europe-west6 \
    gs://dtc_data_lake_datazoomcamp-375017/code/spark-dataproc-cluster.py \
    -- \
    --input_green=gs://dtc_data_lake_datazoomcamp-375017/pq/green/2020/* \
    --input_yellow=gs://dtc_data_lake_datazoomcamp-375017/pq/yellow/2020/* \
    --output=gs://dtc_data_lake_datazoomcamp-375017/report/revenue-2020


// using gcloud cli , this is command to run python spark file to save results to bigquery table
// jars needed for bigquery access for dataproc
// ensure you have the code/file already copied to the gcs location
gcloud dataproc jobs submit pyspark \
    --cluster=dezoomcamp-cluster \
    --region=europe-west6 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc_data_lake_datazoomcamp-375017/code/spark-save-results-to-bigquery.py \
    -- \
    --input_green=gs://dtc_data_lake_datazoomcamp-375017/pq/green/2020/* \
    --input_yellow=gs://dtc_data_lake_datazoomcamp-375017/pq/yellow/2020/* \
    --output=trips_data_all.reports-2020