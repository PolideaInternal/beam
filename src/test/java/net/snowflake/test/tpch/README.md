# TPC-H Tests

This package contains SnowflakeIO integration tests for reading from and writing to Snowflake, using [TPC-H](https://docs.snowflake.net/manuals/user-guide/sample-data-tpch.html) dataset.

There are two scenarios:
- reading/writing entire table
- reading/writing selective query

To support those scenarios, there are two Read test cases:
- `TpchReadTableTest` reading entire `LINEITEM` table
- `TpchReadQueryTest` reading part of `LINEITEM` table via `SELECT L_ORDERKEY, L_PARTKEY, L_COMMENT FROM LINEITEM` query.


Both of them are storing data as [Parquet](http://parquet.apache.org/) files on [Google Cloud Storage](https://cloud.google.com/storage/).

There are also two Write test cases, operating on Parquet files from Read scenarios:
- `TpchWriteTableTest` writing entire `LINEITEM` table
- `TpchWriteQueryTest` Writing part of `LINEITEM` table via query

The tests should be used in pairs:
- `TpchReadTableTest` -> `TpchWriteTableTest` 
- `TpchReadQueryTest` -> `TpchWriteQueryTest` 

### Prerequisites
Before running the tests, some steps has to be made

#### 1. Create GCS bucket
To create GCS bucket, run following command:
```shell script
gsutil mb gs://my-globally-unique-bucket-name
```
Additional parameters can be specified, please see the [documentation](https://cloud.google.com/storage/docs/creating-buckets#storage-create-bucket-gsutil)

#### 2. Create Storage Integration
To create STORAGE INTEGRATION please run following Snowflake query
```sql
CREATE STORAGE INTEGRATION <INTEGRATION-NAME>
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://<BUCKET NAME>');
```

*NOTE*: Please note that `gcs` prefix is used here, not `gs`.

#### 3. Authorize Snowflake to operate on your bucket
Once Storage Integration is created, please follow [this guide](https://docs.snowflake.net/manuals/user-guide/data-load-gcs-config.html)
to authorize Snowflake to operate on your GCS bucket. 

## Running Read Table test

```shell script
./gradlew test --tests TpchReadTableTest -DintegrationTestPipelineOptions='[
"--serverName=<YOUR SNOWFLAKE SERVER NAME>", 
"--username=<USERNAME>", 
"--password=<PASSWORD>", 
"--parquetFilesLocation=gs://<BUCKET-NAME>/table-parquet/", 
"--externalLocation=gs://<BUCKET-NAME>/table-data/", 
"--storageIntegration=<STORAGE INTEGRATION NAME>", 
"--runner=DataflowRunner", 
"--project=<GCP_PROJECT>", 
"--testSize=TPCH_SF1000", 
"--tempLocation=gs://<BUCKET-NAME>/dataflow-read-table-tmp"]'
```

## Running Read Query test

```shell script
./gradlew test --tests TpchReadQueryTest -DintegrationTestPipelineOptions='[
"--serverName=<YOUR SNOWFLAKE SERVER NAME>", 
"--username=<USERNAME>", 
"--password=<PASSWORD>", 
"--parquetFilesLocation=gs://<BUCKET-NAME>/query-parquet/",
"--externalLocation=gs://<BUCKET-NAME>/query-data/", 
"--storageIntegration=<STORAGE INTEGRATION NAME>", 
"--runner=DataflowRunner", 
"--project=<GCP_PROJECT>", 
"--testSize=TPCH_SF1000",
"--tempLocation=gs://<BUCKET-NAME>/dataflow-read-query-tmp"]'
```

## Running Write Table test

Prepare target table in the same schema as TPC-H `LINEITEM` table:
```sql
CREATE TABLE "<DATABASE>"."<SCHEMA>".<TABLE_NAME> LIKE "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM";
```

run the test
```shell script
./gradlew test --tests TpchWriteTableTest -DintegrationTestPipelineOptions='[
"--serverName=<YOUR SNOWFLAKE SERVER NAME>", 
"--username=<USERNAME>", 
"--password=<PASSWORD>", 
"--database=<DATABASE>", 
"--table=<TABLE NAME>", 
"--schema=<SCHEMA>", 
"--parquetFilesLocation=gs://<BUCKET-NAME>/table-parquet/*", 
"--externalLocation=gs://<BUCKET-NAME>/csv-table-location/", 
"--storageIntegration=<STORAGE INTEGRATION NAME>", 
"--runner=DataflowRunner", 
"--project=<GCP_PROJECT>", 
"--tempLocation=gs://<BUCKET-NAME>/dataflow-write-table-tmp"]'
```


## Running Write Query test

Prepare stage:
```
CREATE STAGE <STAGE NAME> 
    STORAGE_INTEGRATION = <STORAGE INTEGRATION> 
    URL = 'gcs://<BUCKET-NAME>/csv-query-location';
```
Please note that `URL` has the `gcs` prefix for creating stage.

No need to prepare table, it will be automatically created.

run the test
```shell script
./gradlew test --tests TpchWriteQueryTest -DintegrationTestPipelineOptions='[
"--serverName=<YOUR SNOWFLAKE SERVER NAME>", 
"--username=<USERNAME>", 
"--password=<PASSWORD>", 
"--database=<DATABASE>", 
"--table=<TABLE NAME>", 
"--schema=<SCHEMA>", 
"--parquetFilesLocation=gs://<BUCKET-NAME>/table-parquet/*", 
"--externalLocation=gs://<BUCKET-NAME>/csv-query-location/", 
"--stage=<STAGE NAME>", 
"--runner=DataflowRunner", 
"--project=<GCP_PROJECT>", 
"--tempLocation=gs://<BUCKET-NAME>/dataflow-write-table-tmp"]'
```
