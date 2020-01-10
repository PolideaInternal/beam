# SnowflakeIO for Beam
This project contains [Beam IO](https://beam.apache.org/documentation/io/built-in/) transforms for reading data from 
and writing data to [Snowflake](https://www.snowflake.com/) tables. The SnowflakeIO transforms use the 
[Snowflake JDBC Driver](https://github.com/snowflakedb/snowflake-jdbc).

For more information about Snowflake, see the [Snowflake documentation](https://docs.snowflake.net/manuals/index.html).
 
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

## Example run whole project

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
        --externalLocation=<GCS LOCATION STARTING WITH gcs://...> 
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
        "--externalLocation=<GCS LOCATION STARTING WITH gcs://...>", 
        "--integrationName=<SNOWFLAKE INTEGRATION NAME>", 
        "--runner=<DirectRunner/DataflowRunner>", 
        "--project=<FOR DATAFLOW RUNNER: GCP PROJECT NAME>", 
        "--tempLocation=<FOR DATAFLOW RUNNER: GCS TEMP LOCATION STARTING WITH gs://...",  
        "--region=<FOR DATAFLOW RUNNER: GCP REGION>", 
        "--appName=<OPTIONAL: DATAFLOW JOB NAME PREFIX", 
        ...]' 
``` 
