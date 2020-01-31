# SnowflakeIO for Beam
This project contains [Beam IO](https://beam.apache.org/documentation/io/built-in/) transforms for reading data from 
and writing data to [Snowflake](https://www.snowflake.com/) tables. The SnowflakeIO transforms use the 
[Snowflake JDBC Driver](https://github.com/snowflakedb/snowflake-jdbc).

For more information about Snowflake, see the [Snowflake documentation](https://docs.snowflake.net/manuals/index.html).

# Table of contents
* [Authentication](#authentication)
* [DataSource Configuration](#datasource-configuration)
* [Reading from Snowflake](#reading-from-snowflake)
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
--username=<USERNAME> --password=<PASSWORD
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

###General usage
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

* `.withUrl(...)` 
JDBC-like URL for your Snowflake account, including account name and region, without any parameters.
* `.withServerName(...)`
Server Name - full server name with account, zone and domain.
* `.withDatabase(...)`
Name of the Snowflake database to use. 
* `.withWarehouse(...)`
Name of the Snowflake warehouse to use. This parameter is optional. If no warehouse name is specified, the default 
warehouse for the user is used.
* `.withSchema(...)`
Name of the schema in the database to use. This parameter is optional.

**Note** - either `.withUrl(...)` or `.withServerName(...)` is required.


## Reading from Snowflake
One of the functions of SnowflakeIO is reading Snowflake tables - either full tables via table name or custom data 
via query. Output of the read transform is a [PCollection](https://beam.apache.org/releases/javadoc/2.17.0/org/apache/beam/sdk/values/PCollection.html) 
of user-defined data type.

###General usage
The basic `.read()` operation usage:

```
PCollection<USER_DATA_TYPE> items = pipeline.apply(
   SnowflakeIO.<USER_DATA_TYPE>read()
       .withDataSourceConfiguration(dc)
       .fromTable("MY_TABLE") // or .fromQuery("QUERY")
       .withExternalLocation("GSC PATH")
       .withIntegrationName("STORAGE INTEGRATION NAME")
       .withCsvMapper(MAPPER_TO_USER_DATA_TYPE)
       .withCoder(BEAM_CODER_FOR_USER_DATA_TYPE));
)
```

Where all below parameters are required:
* `.withDataSourceConfiguration(...)` 
accepts a [DataSourceConfiguration](#datasource-configuration) object.
* `.fromTable(...)` or `.fromQuery(...)`
specifies a Snowflake table name or custom SQL query.
* `.withExternalLocation(...)`
accepts the path to a Google Cloud Storage bucket. 
* `.withIntegrationName(...)`
accepts the name of a Snowflake storage integration object configured for the GCS bucket specified in the  
 `.withExternalLocation` parameter.
* `.withCsvMapper(mapper)`
accepts a [CSVMapper](#csvmapper) instance for mapping `String[]` to USER_DATA_TYPE.
* `withCoder(coder)`
accepts the [Coder](https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/coders/Coder.html) 
for USER_DATA_TYPE.

####CSVMapper
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
        --integrationName=<SNOWFLAKE INTEGRATION NAME> 
        --externalLocation=<GCS LOCATION STARTING WITH gs://...> 
        --runner=<DirectRunner/DataflowRunner>
        --project=<FOR DATAFLOW RUNNER: GCP PROJECT NAME> 
        --tempLocation=<FOR DATAFLOW RUNNER: GCS TEMP LOCATION STARTING WITH gs://...  
        --region=<FOR DATAFLOW RUNNER: GCP REGION> 
        --appName=<OPTIONAL: DATAFLOW JOB NAME PREFIX 
    "
```

It is possible to add integrationName and externalLocation directly to the Pipeline.

The other option is to create tests directly in this project using JUnit4, similar to [those](https://gitlab.polidea.com/snowflake-beam/snowflake/blob/master/src/test/java/com/polidea/snowflake/test/BatchWriteTest.java).
Then run:
```
./gradlew  test 
    -DintegrationTestPipelineOptions='[
        "--serverName=<SNOWFLAKE SERVER NAME>“,  
        "--username=<SNOWFLAKE USERNAME>", 
        "--password=<SNOWFLAKE PASSWORD>", 
        "--output=<INTERNAL OR EXTERNAL LOCATION FOR SAVING OUTPUT FILES>",  
        "--externalLocation=<GCS LOCATION STARTING WITH gs://...>", 
        "--integrationName=<SNOWFLAKE INTEGRATION NAME>", 
        "--runner=<DirectRunner/DataflowRunner>", 
        "--project=<FOR DATAFLOW RUNNER: GCP PROJECT NAME>", 
        "--tempLocation=<FOR DATAFLOW RUNNER: GCS TEMP LOCATION STARTING WITH gs://...",  
        "--region=<FOR DATAFLOW RUNNER: GCP REGION>", 
        "--appName=<OPTIONAL: DATAFLOW JOB NAME PREFIX", 
        ...]' 
``` 
