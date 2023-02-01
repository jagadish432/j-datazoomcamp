## DataLake vs DataWarehouse
    DataLake
	1. large datasets, Raw, undefined
	2. unstructured
	3. data scientists and data analysts
	4. acessible to everyone
	5. cheap storage
	
    DataWarehouse
	1. strutcured datasets
	2. Business analysts
	3. Batch processing & BI, Reporting
	4. Refined, smaller, relational

## ETL vs ELT
    ETL - Export/Extract Transform Load - like DataWarehouse
    ELT - Export/Extract Load Transform - like DataLake
	
## cloud providers features for DataLake
    GCP - cloud storage
    AWS - S3
    Azure - Azure BLOB

## cloud providers features for DataWarehouse
    GCP - BigQuery
    AWS - RedShift
    Azure - Microsoft Azure Synapse Analytics

### notes
1. Workflow Orchestration
	- governing the dataflow in a way that it respects orchestration roles and our business logic
2. Dataflow
	- is that binds otherwise disparates the set of applications together
3. Orchestration tool
	- allows to turn any code into a workflow that we can schedule, run and observe

### prefect deployment commands
1. deployment build(generate yaml file) - `prefect deployment build ./flows/03_deployments/parameterized_flow.py:etl_parent_flow -n "Deploy Parameterized ETL job"`
2. deployment apply(push yaml to prefect) - `prefect deployment apply etl_parent_flow-deployment.yaml`
   a. now, start the deployment by clicking on "Run > Quick run" from the prefect UI > deployments > required deployment.
3. docker build image - `docker build -t <docker_userid>/<custom_image_name>:<custom_tag> <relative path where the Dockerfile exists> (eg: docker build -t 4329/prefect:de-zoomcamp .)
4. login to docker using cli - `docker login` -> will prompt for docker user ID and password
5. push image to docker hub - `docker push <docker_userid>/<custom_image_name>:<custom_tag>` (eg: docker push 4329/prefect:de-zoomcamp)
	a. once done, we should be able to view our new image in the docker hub portal - `https://hub.docker.com/`
6. create a prefect block for docker - either from UI or using python code (`blocks/make_docker_block.py`)
7. To start prefect Agent - `prefect agent start -q default`
8. set PREFECT_API_URL so the docker container knows where to call for prefect deployments - `prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"`
9. to see prefect profiles - `prefect profile ls` (like an aws config)
10. Build and apply the prefect deployment using python code - `python flows/03_deployments/docker_deploy.py`
11. start prefect deployment - either from prefect UI, OR using cmd - `prefect deployment run etl-parent-flow/docker-flow`
