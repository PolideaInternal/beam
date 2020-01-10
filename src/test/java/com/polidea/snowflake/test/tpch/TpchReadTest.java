package com.polidea.snowflake.test.tpch;

import com.polidea.snowflake.io.SnowflakeIO;
import com.polidea.snowflake.io.credentials.SnowflakeCredentialsFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/*
This test suite uses SNOWFLAKE_SAMPLE_DATA database containing TPC-H dataset, explained here
https://docs.snowflake.net/manuals/user-guide/sample-data-tpch.html

This suite contains two tests
- reading entire `LINEITEM` table
- running query

By default, tests are using TPCH_SF1_1 dataset. Use `--testSize` pipeline option to modify it.

Once data is read from Snowflake, it saved to GCS as Parquet files using standard Beam FileIO.

To run this test on Dataflow, un-ignore one test and execute following command:

./gradlew  test  -DintegrationTestPipelineOptions='["--serverName=<SNOWFLAKE SERVER NAME>", "--username=<USERNAME>",
"--password=<PASSWORD>", "--output=<GCS PATH FOR OUTPUT DATA>", "--externalLocation=<GCS PATH TO USE AS STAGE>",
"--integrationName=<SNOWFLAKE INTEGRATION NAME RELATED TO --externalLocation>", "--testSize=TPCH_SF1_1000"
"--runner=DataflowRunner", "--project=<GCP PROJECT ID>",
"--tempLocation=<"TMP LOCATION ON GCP FOR DATAFLOW">"]' --no-build-cache

 */
public class TpchReadTest {
  private static final String DATABASE = "SNOWFLAKE_SAMPLE_DATA";
  private static final String TABLE = "LINEITEM";
  private static final String QUERY = "SELECT * FROM LINEITEM";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static TpchTestPipelineOptions options;
  private static String externalLocation;
  private static String integrationName;
  private static String output;

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(TpchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(TpchTestPipelineOptions.class);
    externalLocation = options.getExternalLocation();
    integrationName = options.getIntegrationName();
    output = options.getOutput();

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withUrl(options.getUrl())
            .withServerName(options.getServerName())
            .withWarehouse(options.getWarehouse())
            .withDatabase(DATABASE)
            .withSchema(options.getTestSize()); // selecting tests size is done by selecting schema
  }

  @Test
  @Ignore
  public void tpchReadTestForTable() {
    PCollection<GenericRecord> items =
        pipeline.apply(
            SnowflakeIO.<GenericRecord>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromTable(TABLE)
                .withExternalLocation(externalLocation)
                .withIntegrationName(integrationName)
                .withCsvMapper(TpchTestUtils.getCsvMapper())
                .withCoder(AvroCoder.of(TpchTestUtils.getSchema())));

    items.apply(
        FileIO.<GenericRecord>write().via(ParquetIO.sink(TpchTestUtils.getSchema())).to(output));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void tpchReadTestForQuery() {
    PCollection<GenericRecord> items =
        pipeline.apply(
            SnowflakeIO.<GenericRecord>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromQuery(QUERY)
                .withExternalLocation(externalLocation)
                .withIntegrationName(integrationName)
                .withCsvMapper(TpchTestUtils.getCsvMapper())
                .withCoder(AvroCoder.of(TpchTestUtils.getSchema())));

    items.apply(
        FileIO.<GenericRecord>write().via(ParquetIO.sink(TpchTestUtils.getSchema())).to(output));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
