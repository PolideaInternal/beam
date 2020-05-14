# SnowflakeIO for Beam
This project contains [Beam IO](https://beam.apache.org/documentation/io/built-in/) transforms for reading data from 
and writing data to [Snowflake](https://www.snowflake.com/) tables. The SnowflakeIO transforms use the 
[Snowflake JDBC Driver](https://github.com/snowflakedb/snowflake-jdbc).

For more information about Snowflake, see the [Snowflake documentation](https://docs.snowflake.net/manuals/index.html).

# Table of contents
* [Authentication](#authentication)
* [DataSource Configuration](#datasource-configuration)
* [Reading from Snowflake](#reading-from-snowflake)
* [Writing to Snowflake](#writing-to-snowflake)
* [Location](#location)
* [Pipeline options](#pipeline-options)
* [Using SnowflakeIO jar](#using-snowflakeio-jar)

## Authentication
All authentication methods available for the Snowflake JDBC Driver are possible to use with the IO transforms:
* Username and password
* Key pair
* OAuth token

Passing credentials is done via Pipeline options used to instantiate `SnowflakeIO.DataSourceConfiguration`:
```
SnowflakePipelineOptions options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(SnowflakePipelineOptions.class);
SnowflakeCredentials credentials = SnowflakeCredentialsFactory.of(options);

SnowflakeIO.DataSourceConfiguration.create(credentials)
        .(other DataSourceConfiguration options)
```

### Username and password
To use username/password authentication in SnowflakeIO, invoke your pipeline with the following Pipeline options:
```
--username=<USERNAME> --password=<PASSWORD>
```

### Key pair

**Note**: 
To use this authentication method, you must first generate a key pair and associate the public key with the Snowflake 
user that will connect using the IO transform. For instructions,  see the [Snowflake documentation](https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html#using-key-pair-authentication).

To use key pair authentication with SnowflakeIO, invoke your pipeline with following Pipeline options:
```
--username=<USERNAME> --privateKeyPath=<PATH_TO_P8_FILE> --privateKeyPassphrase=<PASSWORD_FOR_KEY>
```

### OAuth token
SnowflakeIO also supports OAuth token.  

**IMPORTANT**: SnowflakeIO requires a valid OAuth access token. It will neither be able to refresh the token nor obtain 
it using a web-based flow. For information on configuring an OAuth integration and obtaining the token, see the 
[Snowflake documentation](https://docs.snowflake.net/manuals/user-guide/oauth-intro.html).

Once you have the token, invoke your pipeline with following Pipeline Options: 
```
--oauthToken=<TOKEN>
```

## DataSource Configuration

DataSource configuration is required in both read and write object for configuring Snowflake connection properties 
for IO purposes.

### General usage
Create the DataSource configuration::
```
       SnowflakeIO.DataSourceConfiguration
            .create(SnowflakeCredentialsFactory.of(options))
            .withUrl(options.getUrl())
            .withServerName(options.getServerName())
            .withDatabase(options.getDatabase())
            .withWarehouse(options.getWarehouse())
            .withSchema(options.getSchema());
```
Where parameters can be:

* `.withUrl(...)` JDBC-like URL for your Snowflake account, including account name and region, without any parameters.
* `.withServerName(...)` Server Name - full server name with account, zone and domain.
* `.withDatabase(...)` Name of the Snowflake database to use. 
* `.withWarehouse(...)` Name of the Snowflake warehouse to use. This parameter is optional. If no warehouse name is specified, the default  warehouse for the user is used.
* `.withSchema(...)` Name of the schema in the database to use. This parameter is optional.

**Note** - either `.withUrl(...)` or `.withServerName(...)` is required.

## Reading from Snowflake
One of the functions of SnowflakeIO is reading Snowflake tables - either full tables via table name or custom data 
via query. Output of the read transform is a [PCollection](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/values/PCollection.html) 
of user-defined data type.

### General usage
The basic `.read()` operation usage:

```
Location location = Location.of("STORAGE INTEGRATION NAME", "GSC BUCKET NAME");
PCollection<USER_DATA_TYPE> items = pipeline.apply(
   SnowflakeIO.<USER_DATA_TYPE>read()
       .withDataSourceConfiguration(dc)
       .fromTable("MY_TABLE") // or .fromQuery("QUERY")
       .via(location)
       .withCsvMapper(mapper)
       .withCoder(coder));
)
```

Where all below parameters are required:
* `.withDataSourceConfiguration(...)` accepts a [DataSourceConfiguration](#datasource-configuration) object.
* `.fromTable(...)` or `.fromQuery(...)` specifies a Snowflake table name or custom SQL query.
* `.via(...)` accepts a Location object.
* `.withCsvMapper(mapper)` accepts a [CSVMapper](#csvmapper) instance for mapping `String[]` to USER_DATA_TYPE.
* `.withCoder(coder)` accepts the [Coder](https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/coders/Coder.html) for USER_DATA_TYPE. 


#### CSVMapper
SnowflakeIO uses a [COPY INTO <location>](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html) 
statement to move data from a Snowflake table to Google Cloud Storage as CSV files. These files are then downloaded 
via [FileIO](https://beam.apache.org/releases/javadoc/2.3.0/index.html?org/apache/beam/sdk/io/FileIO.html) and 
processed line by line. Each line is split into an array of Strings using the [OpenCSV](http://opencsv.sourceforge.net/)
library. 

The CSVMapper’s job is to give the user the possibility to convert the array of Strings to a user-defined type, ie. GenericRecord for Avro or Parquet files, or custom POJO.

Example implementation of CSVMapper for GenericRecord:

```
static SnowflakeIO.CsvMapper<GenericRecord> getCsvMapper() {
   return (SnowflakeIO.CsvMapper<GenericRecord>)
           parts -> {
               return new GenericRecordBuilder(PARQUET_SCHEMA)
                       .set("ID", Long.valueOf(parts[0]))
                       .set("NAME", parts[1])
                       [...]
                       .build();
           };
}
```

## Writing to Snowflake

One of the functions of SnowflakeIO is writing to Snowflake tables. This transformation enables you to finish the Beam pipeline with an output operation that sends the user's [PCollection](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/values/PCollection.html) to your Snowflake database.

### General usage

#### Batch write (from a bounded source)

The basic .write() operation usage is as follows:
```
Location location = Location.of("STORAGE INTEGRATION NAME", "GSC BUCKET NAME");
data.apply(
   SnowflakeIO.<type>write()
       .withDataSourceConfiguration(dc)
       .to("MY_TABLE")
       .via(location)
       .withUserDataMapper(mapper)
)
```

Replace type with the data type of the PCollection object to write; for example, SnowflakeIO.<String> for an input PCollection of Strings.

All the below parameters are required:
* `.withDataSourceConfiguration()` - accepts a DatasourceConfiguration object.
* `.to()` - accepts the target Snowflake table name.
* `.via()` - accepts a Location object.
* `.withUserDataMapper()` - accepts the UserDataMapper function that will map a user's PCollection to an array of String values (`String[]`).

#### Streaming write (from an unbounded source)

It is required to create a [SnowPipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe.html) in the Snowflake console. SnowPipe should use the same integration and the same bucket as specified in the location object. The write operation might look as follows:

```
data.apply(
   SnowflakeIO.<type>write()
	.via(location)
      .withDataSourceConfiguration(dc)
      .withUserDataMapper(mapper)
      .withSnowPipe("MY_SNOW_PIPE")
      .withFlushTimeLimit(Duration.millis(time))
      .withFlushRowLimit(rowsNumber)
      .withShardsNumber(shardsNumber)
)
```

##### Parameters

**Required** for streaming:
* `.withDataSourceConfiguration()` - accepts a DatasourceConfiguration object.
* `.to()` - accepts the target Snowflake table name.
* `.via()` - accepts a Location object.
* `.withSnowPipe()` - accepts the target SnowPipe name
* `.withUserDataMapper()` - accepts the UserDataMapper function that will map a user's PCollection to an array of String values (`String[]`).

**Optional** for streaming:

* `.withFlushTimeLimit()` - accepts Duration objects with the specified time after each the streaming write will be repeated. Default value: 30 seconds.
* `.withFlushRowLimit()` - accepts limit number of rows written to each file staged file. Default value: 10,000 rows.
* `.withShardNumber()` - accepts number of files that will be saved in every flush (for purposes of parallel write). Default value: 1 shard.

**Important notice**: Streaming accepts only key pair **authentication**.

##### Flush time: duration & number of rows

Duration: streaming write will write periodically files on stage according to time duration specified in flush time limit (for example. every 1 minute). 

Number of rows: files staged for write will have number of rows specified in flush row limit unless the flush time limit will be reached (for example if the limit is 1000 rows and buffor collected 99 rows and the 1 minute flush time passes, the rows will be sent to SnowPipe for insertion).

Size of staged files will depend on the rows size and used compression (GZIP).

### UserDataMapper function

The UserDataMapper function is required to map data from a PCollection to an array of String values before the write() operation saves the data to temporary .csv files. For example:

```
public static SnowflakeIO.UserDataMapper<Long> getCsvMapper() {
    return (SnowflakeIO.UserDataMapper<Long>) recordLine -> new String[] {recordLine.toString()};
}
```

### Additional write options

#### Transformation query

The .withQueryTransformation() option for the write() operation accepts a SQL query as a String value, which will be performed while transfering data staged in CSV files directly to the target Snowflake table. For information about the transformation SQL syntax,  see the [Snowflake Documentation](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html#transformation-parameters).

Usage:

```
String query = "SELECT t.$1 from YOUR_TABLE;";
data.apply(
   SnowflakeIO.<~>write()
       .withDataSourceConfiguration(dc)
       .to("MY_TABLE")
       .via(location)
       .withUserDataMapper(mapper)
       .withQueryTransformation(query)
)
```

#### Write disposition

Define the write behaviour based on the table where data will be written to by specifying the .withWriteDisposition(...) option for the write() operation. The following values are supported:
* APPEND - Default behaviour. Written data is added to the existing rows in the table,
* EMPTY - The target table must be empty;  otherwise, the write operation fails,
* TRUNCATE - The write operation deletes all rows from the target table before writing to it.

Example of usage:
```
data.apply(
   SnowflakeIO.<~>write()
       .withDataSourceConfiguration(dc)
       .to("MY_TABLE")
       .via(location)
       .withUserDataMapper(mapper)
       .withWriteDisposition(TRUNCATE)
)
```

#### Create disposition

The `.withCreateDisposition()` option defines the behavior of the write operation if the target table does not exist . The following values are supported:
* CREATE_IF_NEEDED - default behaviour. The write operation checks whether the specified target table exists; if it does not, the write operation attempts to create the table Specify the schema for the target table using the `.withTableSchema()` option.
* CREATE_NEVER -  The write operation fails if the target table does not exist.

Usage:
```
data.apply(
   SnowflakeIO.<~>write()
       .withDataSourceConfiguration(dc)
       .to("MY_TABLE")
       .via(location)
       .withUserDataMapper(mapper)
       .withCreateDisposition(CREATE_NEVER)
)
```

#### Table schema disposition

When the `.withCreateDisposition()` .option is set to CREATE_IF_NEEDED, the `.withTableSchema()` option enables specifying the schema for the created target table. 
A table schema is a list of `SnowflakeColumn` objects with name and type corresponding to [column type](https://docs.snowflake.net/manuals/sql-reference/data-types.html) for each column in the table. 

Usage:
```
SnowflakeTableSchema tableSchema =
    new SnowflakeTableSchema(
        SnowflakeColumn.of("my_date", new SnowflakeDate(), true),
        new SnowflakeColumn("id", new SnowflakeNumber()),
        SnowflakeColumn.of("name", new SnowflakeText(), true));

data.apply(
   SnowflakeIO.<~>write()
       .withDataSourceConfiguration(dc)
       .to("MY_TABLE")
       .via(location)
       .withUserDataMapper(mapper)
       .withTableSchema(tableSchema)
)
```

## Location 

Location object is used as argument by [write](#writing-from-snowflake) and [read](#reading-from-snowflake).

There are two ways of creating Location object:
* By passing `stagingBucketName` and `storageIntegration`. `stagingBucketName` is name of the Google Cloud Storage bucket 
and `storageIntegration` is the name of a Snowflake storage integration object created according to [Snowflake documentation](https://docs.snowflake.net/manuals/sql-reference/sql/create-storage-integration.html) for the GCS bucket. 
```
Location location = Location.of(storageIntegration, stagingBucketName)`
```
* By passing `SnowflakePipelineOptions` object which must contain `stagingBucketName` and `storageIntegration`.
```
Location location = Location.of(options)
```

General notes:
* Currently, this library only supports Google Cloud Storage for external storage. Please be aware of [Google Cloud Storage Billing](https://cloud.google.com/storage/pricing) while using it as an integration.
Some data transfer billing charges may apply when loading data from files staged across different platforms. For more information, see [Understanding Snowflake Data Transfer Billing](https://docs.snowflake.net/manuals/user-guide/billing-data-transfer.html). 
An administrator must configure a Snowflake integration object to allow Snowflake to read data from and write to a Google Cloud Storage bucket. For instructions, see the  [Snowflake documentation](https://docs.snowflake.net/manuals/user-guide/data-load-gcs-config.html).

Write notes:
* SnowflakeIO uses COPY statements behind the scenes to write (using [COPY to table](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html)) 
* `stagingBucketName` will be used to save CSV files which will end up in Snowflake . 
Those CSV files will be saved under `stagingBucketName/data` path.

Read notes:
* SnowflakeIO uses COPY statements behind the scenes to read (using [COPY to location](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html)) files staged in cloud storage.
* `stagingBucketName` will be used as temporary location for storing CSV files. 
Those temporary directories will be named `sf_copy_csv_DATE_TIME_RANDOMSUFFIX` 
and they will be removed automatically once Read operation finishes.

## Pipeline options

Use Beam’s [Pipeline options](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/options/PipelineOptions.html) to set options via the command line.

### Snowflake Pipeline options

In the Snowflake IO library, the SnowflakePipelineOptions class defines all options that can be passed via the command line by default when a Pipeline uses them.

### Using Pipeline options

#### Pipelines in source code - (./gradle run execution)

To use Pipeline options, you must configure them as follows:
```
SnowflakePipelineOptions options = PipelineOptionsFactory
    .fromArgs(args)
    .withValidation()
    .as(SnowflakePipelineOptions.class);
```
All the below parameters are required:
* `.fromArgs()` - GNU style command line arguments, for example: `--project=myproject --x=1`
* `.withValidation()` - which validates that PipelineOptions confirms all criteria from the passed in interface
* `.as()` - a class of used pipeline options

Then create your pipeline using created options:

`Pipeline pipeline = Pipeline.create(options);`

Example of accessing pipeline options in code:


`String stagingBucketName = options.getStagingBucketName();`
#### Pipelines in tests:

```
PipelineOptionsFactory.register(SnowflakePipelineOptions.class);
    options = TestPipeline
                .testingPipelineOptions()                                  
                .as(SnowflakePipelineOptions.class);
```

Then use them in pipeline run:

`PipelineResult pipelineResult = pipeline.run(options);` 

### Extending pipeline options
Extend the SnowflakePipelineOptions with your own custom options to access additional parameters in your code.

Example of extending the Pipeline options:

```
public interface BatchTestPipelineOptions extends SnowflakePipelineOptions {
  @Description("Table name to connect to.")
  String getTable();

  void setTable(String table);
}
```

Note: in case extending of extending Pipeline Options remember to use it in your code:
```
ExampleSnowflakePipelineOptions options = PipelineOptionsFactory
.fromArgs(args)
.withValidation()
.as(ExampleSnowflakePipelineOptions.class);
```

And in case of testing:
```
PipelineOptionsFactory.register(ExampleSnowflakePipelineOptions.class);
    options = TestPipeline
    .testingPipelineOptions()
                .as(ExampleSnowflakePipelineOptions.class);
```

### Running main command with Pipeline options
To pass Pipeline options via the command line, use `--args` in a gradle command as follows:

```
./gradle run 
    --args="
        --serverName=<SNOWFLAKE SERVER NAME>  
        --username=<SNOWFLAKE USERNAME> 
        --password=<SNOWFLAKE PASSWORD>  
        --database=<SNOWFLAKE DATABASE> 
        --schema=<SNOWFLAKE SCHEMA> 
        --table=<SNOWFLAKE TABLE IN DATABASE> 
        --query=<IF NOT TABLE THEN QUERY> 
        --storageIntegration=<SNOWFLAKE STORAGE INTEGRATION NAME> 
        --stagingBucketName=<GCS BUCKET NAME> 
        --runner=<DirectRunner/DataflowRunner>
        --project=<FOR DATAFLOW RUNNER: GCP PROJECT NAME> 
        --tempLocation=<FOR DATAFLOW RUNNER: GCS TEMP LOCATION STARTING WITH gs://...>
        --region=<FOR DATAFLOW RUNNER: GCP REGION> 
        --appName=<OPTIONAL: DATAFLOW JOB NAME PREFIX 
    "
```

Then in the code it is possible to access the parameters with arguments using the `options.getStagingBucketName();` command.

### Running tests command with pipeline options

To pass pipeline options via the command line, use `-DintegrationTestPipelineOptions` in a gradle command as follows:

```
./gradlew test --tests nameOfTest 
-DintegrationTestPipelineOptions='[
  --serverName=<SNOWFLAKE SERVER NAME>,
  "--username=<SNOWFLAKE USERNAME", 
  "--password=<SNOWFLAKE PASSWORD", 
  "--schema=<SNOWFLAKE SCHEMA>", 
  "--table=<SNOWFLAKE TABLE IN DATABASE>", 
  "--database=<SNOWFLAKE DATABASE>",
  "--storageIntegration=<SNOWFLAKE STORAGE INTEGRATION NAME>",
  "--stagingBucketName=<GCS BUCKET NAME>",
]' --no-build-cache
```

### Running pipelines on Dataflow

By default, pipelines are run on [Direct Runner](https://beam.apache.org/documentation/runners/direct/) on your local machine. To run a pipeline on [Google Dataflow](https://cloud.google.com/dataflow/), you must provide the following Pipeline options:
* `--runner=DataflowRunner` - Name of a specific runner. Alternatively, use the DirectRunner option.
* `--project=gcs-project` - Name of the Google Cloud Platform project.
* `--stagingBucketName=gcs_bucket_name` - Google Cloud Services bucket where the Beam files will be staged.
* `--maxNumWorkers=5` - (optional) Maximum number of workers.
* `--appName=prefix` - (optional) Prefix for the job name in the Dataflow Dashboard.

More pipeline options for Dataflow can be found [here](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/runners/dataflow/options/DataflowPipelineOptions.html).

Note: To properly authenticate with Google Cloud, please use [gcloud](https://cloud.google.com/sdk/gcloud/) or follow the [Google Cloud documentation](https://cloud.google.com/docs/authentication/).

Important: Please acknowledge [Google Dataflow pricing](https://www.google.pl/search?client=opera&q=dataflow+pricing&sourceid=opera&ie=UTF-8&oe=UTF-8). 


## Using SnowflakeIO jar

### Create .jar
To create .jar run:
```./gradlew fatJar```

Then `snowflake-io-all.jar` file will be created in `build/libs/`.

### Using jar

Copy/move file to destination of choice (i.e. `libs/` in the new project).
To use `.jar` file add to `build.gradle` in your project:

```
dependencies {
    compile files('libs/snowflake-io-all.jar')
}
```
To run on pipelines Dataflow add two dependencies:
```
dependencies {
    compile files('libs/snowflake-io-all.jar')
    compile group: 'org.apache.beam', name: 'beam-runners-google-cloud-dataflow-java', version: '2.16.0'
}
```

For creating own pipeline with SnowlakeIO, use operation `read()` or `write()` similarly to following [example](https://gitlab.polidea.com/snowflake-beam/snowflake/blob/master/src/main/java/com/polidea/snowflake/examples/ReadPipelineExample.java).

Then run script using:

```
./gradle run 
    --args="
        --serverName=<SNOWFLAKE SERVER NAME>  
        --username=<SNOWFLAKE USERNAME> 
        --password=<SNOWFLAKE PASSWORD>  
        --database=<SNOWFLAKE DATABASE> 
        --schema=<SNOWFLAKE SCHEMA> 
        --table=<SNOWFLAKE TABLE IN DATABASE> 
        --query=<IF NOT TABLE THEN QUERY> 
        --storageIntegration=<SNOWFLAKE STORAGE INTEGRATION NAME> 
        --stagingBucketName=<GCS BUCKET NAME> 
        --runner=<DirectRunner/DataflowRunner>
        --project=<FOR DATAFLOW RUNNER: GCP PROJECT NAME> 
        --tempLocation=<FOR DATAFLOW RUNNER: GCS TEMP LOCATION STARTING WITH gs://...>
        --region=<FOR DATAFLOW RUNNER: GCP REGION> 
        --appName=<OPTIONAL: DATAFLOW JOB NAME PREFIX 
    "
```

It is possible to add storageIntegration and stagingBucketName directly to the Pipeline.

The other option is to create tests directly in this project using JUnit4, similar to [those](https://gitlab.polidea.com/snowflake-beam/snowflake/blob/master/src/test/java/com/polidea/snowflake/test/BatchWriteTest.java).
Then run:
```
./gradlew  test 
    -DintegrationTestPipelineOptions='[
        "--serverName=<SNOWFLAKE SERVER NAME>“,  
        "--username=<SNOWFLAKE USERNAME>", 
        "--password=<SNOWFLAKE PASSWORD>", 
        "--output=<EXTERNAL LOCATION FOR SAVING OUTPUT FILES>",  
        "--stagingBucketName=<GCS BUCKET NAME", 
        "--storageIntegration=<SNOWFLAKE STORAGE INTEGRATION NAME>", 
        "--runner=<DirectRunner/DataflowRunner>",
        "--project=<FOR DATAFLOW RUNNER: GCP PROJECT NAME>", 
        "--tempLocation=<FOR DATAFLOW RUNNER: GCS TEMP LOCATION STARTING WITH gs://...>",  
        "--region=<FOR DATAFLOW RUNNER: GCP REGION>", 
        "--appName=<OPTIONAL: DATAFLOW JOB NAME PREFIX", 
        ...]' 
``` 
