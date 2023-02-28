# Analytics Engineering

## What is Analytics Engineering?
-> It's an new role that's aiming to bridge the gap between Data engineers and Data analysts/scientists

### Kimball's Dimensional Modelling
1. Prioritize user understanding
2. prioritize query performance

### Elements of Dimensional Modelling
#### Facts Tables
1. Measurements, Metrics and Facts
2. Corresponds to a business process
3. Verbs like Sales, Orders

#### Dimensions Tables
1. Provides context to a business
2. Corresponds to a business entity
3. Nouns like Customers, Products

### Architecture of Dimensional Modelling
#### Stage Area
1. Contains the raw data
2. Not meant to be exposed to everyone

#### Processing Area
1. From raw data to data models
2. Focuses in efficiency
3. Ensuring standards

#### Presentation Area
1. Final presentation of the data
2. Exposure to business stakeholder


## DBT

DBT is a tranformation tool that allows anyone that knows SQL to deploy analytics code following software engineering best practices like modularity, portability, CICD and documentation.

A modeling layer comes into the picture, that has dbt models which transforms the data and persist back to the data warehouse.

### DBT model
Each model is :
1. A sql file
2. Select statement, `no DDL or DML`
3. A file that will compile and run in our DWH(Data WareHouse)

### How to use dbt
#### dbt Core
1. open-source project that allows the data transformation
2. builds and runs a dbt project (sql and yml files)
3. includes sql compilation logic, macros and database adapters
4. includes a cli interface to run dbt commands locally
5. opens source and free to use

#### dbt Cloud 
https://cloud.getdbt.com/
1. SaaS application to develop and manage dbt projects
2. web-based IDE to develop, run and test dbt projects
3. jobs orchestration
4. Logging and Alerting
5. Integrated documentation
6. Free for individuals (1 developer seat)

### How we're going to use the dbt?
#### BigQuery
1. Development using Cloud IDE
2. No local installation of dbt core

#### Postgres
1. Devlopment using local IDE of your choice
2. Local installation of dbt core connecting to the Postgres database
3. Running dbt models through CLI



### Create Schema (raw data) from green and yellow taxi trip data
1. create a dataset (trips_data_all) in gcp console
2. run the below queries to load the data to bigquery

CREATE YELLOW TRIPS TABLE
```
CREATE OR REPLACE EXTERNAL TABLE `datazoomcamp-375017.trips_data_all.yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_datazoomcamp-375017/yellow/yellow_tripdata_*.parquet.gz']
);
```

TEST YELLOW TRIPS TABLE
```
select *
  from `datazoomcamp-375017.trips_data_all.yellow_tripdata`
  where vendorid is not null 
  limit 10
```


CREATE GREEN TRIPS TABLE
```
 CREATE OR REPLACE EXTERNAL TABLE `datazoomcamp-375017.trips_data_all.green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_datazoomcamp-375017/green/green_tripdata_*.parquet.gz']
);
```

TEST GREEN TRIPS TABLE
```
select *
  from `datazoomcamp-375017.trips_data_all.green_tripdata`
  where vendorid is not null 
  limit 10
```


CREATE FHV TRIPS TABLE
```
CREATE OR REPLACE EXTERNAL TABLE `datazoomcamp-375017.trips_data_all.fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_datazoomcamp-375017/fhv/fhv_tripdata_*']
);
```

TEST FHV TRIPS TABLE
```
select *
  from `datazoomcamp-375017.trips_data_all.fhv_tripdata`
  limit 10
```

### Anatomy of a dbt model
#### several materialization strategies
1. Table
2. View
3. Incremental
4. Ephemeral

#### worflow
1. dbt model has SQL instructions along with Jinja format
2. dbt transforms the model and runs compiled code in the Data Warehouse (BigQuery in our case)

#### The FROM clause of a dbt model
##### Sources
1. The data loaded to our DWH that we use as sources for models
2. configuration defined in the yml files in the model folder
3. used with the source macro that will resolve the name to the right schema, plus build the dependencies automatically
4. sources freshness can be defined and tested

##### Seeds
1. CSV files stored in our repository under the seeds folder
2. benefits of version controlling
3. equivalent to a copy command
4. recommended for a data that doesn't change frequently
5. runs with `dbt seed -s <file_name>`

##### Ref
1. Macro to reference the underlying tables and views that were building the data warehouse
2. run the same code in any environment, it'll resolve the correct schema for you
3. dependencies are built automatically


### Build the dbt models
#### staging
1. creating raw models - taking raw models and transforming
2. like type cast, rename columns

#### core
1. models that are exposed to BI tools or stakeholders


### Macros
1. Use control structures (eg: if statements and for loops) in SQL
2. use environment variables in dbt project for production deployment
3. operate on the results of one query to generate another query
4. Kind of a functions in most programming lnaguages - i.e., Abstract sinppets of SQL code into resuable macros
5. Define in separate files under macros folder

### Packages
1. like libraries in other programming languages
2. standalone dbt projects, with models and macros that takle a specific problem area
3. by adding a package, the package's models and macros will be part of your own project
4. imported in packages.yml file and imported by running `dbt steps`
5. a list of useful packages can be found in `dbt package hub`

### Variables
1. useful for defining values that are used across the projects
2. with a macro, dbt allows us to provide data to models for compilation
3. to use a variable, we use {{ var('variable_name') }} function
4. can be defined in:
  a. dbt_project.yml file
  b. on the command line

### Tests
1. tests in dbt are especially a select statements
2. tests are defined on a column in the yml file
3. dbt provides basic tests to check if the column values are:
  Unique, Not Null, Accepted values, A foreign key to another table
4. you can create a custom tests as queries




## My Personal impressions on DBT
1. DBT is a tool which helps us in transforming the raw data to business models
2. yml files in the dbt acts like a ORM(eg: SQLAlchemy) in defining and transforming the table/schema usage
  a. i.e., we can switch to diff envs, sources and yet the code need not be changed
3. DBT structures (especially yml files) is also like a terraform - variables, modules , etc..
4. 


## DBT commands

1. `dbt init` to intialize dbt project with stub
2. `dbt run` to run all models defined/declared in the dbt project or in the current folder
3. `dbt deps` to install all packages mentioned in the `packages.yml` file
4. `dbt seed` to seed/upload the data from csv files under seed folder to the dataset










