package org.apache.beam.sdk.io.snowflake.test.batch;

import static org.junit.Assume.assumeNotNull;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.data.SFColumn;
import org.apache.beam.sdk.io.snowflake.data.SFTableSchema;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFDate;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFDateTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestamp;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestampLTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestampNTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SFTimestampTZ;
import org.apache.beam.sdk.io.snowflake.data.structured.SFArray;
import org.apache.beam.sdk.io.snowflake.data.structured.SFObject;
import org.apache.beam.sdk.io.snowflake.data.structured.SFVariant;
import org.apache.beam.sdk.io.snowflake.data.text.SFText;
import org.apache.beam.sdk.io.snowflake.locations.Location;
import org.apache.beam.sdk.io.snowflake.locations.LocationFactory;
import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SchemaDispositionWriteExternalTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static DataSource dataSource;

  static BatchTestPipelineOptions options;
  static SnowflakeIO.DataSourceConfiguration dc;
  static Location locationSpec;

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @BeforeClass
  public static void setupAll() {
    PipelineOptionsFactory.register(BatchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BatchTestPipelineOptions.class);

    assumeNotNull(options.getServerName());

    dc =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withServerName(options.getServerName())
            .withDatabase(options.getDatabase())
            .withWarehouse(options.getWarehouse())
            .withSchema(options.getSchema());

    dataSource = dc.buildDatasource();
    locationSpec = LocationFactory.of(options);
  }

  @Before
  public void setup() {
    assumeNotNull(options.getExternalLocation());
  }

  @After
  public void tearDown() throws Exception {
    if (options.getExternalLocation() != null) {
      String storageName = options.getExternalLocation();
      storageName = storageName.replaceAll("gs://", "");
      String[] splitted = storageName.split("/", 2);
      String bucketName = splitted[0];
      String path = splitted[1];
      Storage storage = StorageOptions.getDefaultInstance().getService();
      Page<Blob> blobs =
          storage.list(
              bucketName,
              Storage.BlobListOption.currentDirectory(),
              Storage.BlobListOption.prefix(path));

      for (Blob blob : blobs.iterateAll()) {
        storage.delete(blob.getBlobId());
      }
    }
  }

  static class GenerateDates extends DoFn<Long, String[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String date = "2020-08-25";
      String datetime = "2014-01-01 16:00:00";
      String time = "00:02:03";

      String[] listOfDates = {date, datetime, time, datetime, datetime, datetime, datetime};
      c.output(listOfDates);
    }
  }

  static class GenerateNulls extends DoFn<Long, String[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {

      String[] listOfDates = {null, null, null};
      c.output(listOfDates);
    }
  }

  static class GenerateStructuredData extends DoFn<Long, String[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String json = "{ \"key1\": 1, \"key2\": {\"inner_key\": \"value2\", \"inner_key2\":18} }";
      String array = "[1,2,3]";
      String[] listOfDates = {array, json, json};
      c.output(listOfDates);
    }
  }

  @Test
  public void writeToExternalWithCreatedTableWithDatetimeSchemaSuccess() throws SQLException {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    SFTableSchema tableSchema =
        new SFTableSchema(
            SFColumn.of("date", SFDate.of()),
            SFColumn.of("datetime", SFDateTime.of()),
            SFColumn.of("time", SFTime.of()),
            SFColumn.of("timestamp", SFTimestamp.of()),
            SFColumn.of("timestamp_ntz", SFTimestampNTZ.of()),
            SFColumn.of("timestamp_ltz", SFTimestampLTZ.of()),
            SFColumn.of("timestamp_tz", SFTimestampTZ.of()));

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new GenerateDates()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String[]>write()
                .withDataSourceConfiguration(dc)
                .to("test_example_success")
                .withTableSchema(tableSchema)
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(SnowflakeIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("DROP TABLE IF EXISTS test_example_success;"));
  }

  @Test
  public void writeToExternalWithCreatedTableWithNullValuesInSchemaSuccess() throws SQLException {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    SFTableSchema tableSchema =
        new SFTableSchema(
            SFColumn.of("date", SFDate.of(), true),
            new SFColumn("datetime", SFDateTime.of(), true),
            SFColumn.of("text", SFText.of(), true));

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new GenerateNulls()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String[]>write()
                .withDataSourceConfiguration(dc)
                .to("test_example_nulls_success")
                .withTableSchema(tableSchema)
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(SnowflakeIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("DROP TABLE IF EXISTS test_example_nulls_success;"));
  }

  @Test
  public void writeToExternalWithCreatedTableWithStructuredDataSchemaSuccess() throws SQLException {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    SFTableSchema tableSchema =
        new SFTableSchema(
            SFColumn.of("variant", SFArray.of()),
            SFColumn.of("object", SFObject.of()),
            SFColumn.of("array", SFVariant.of()));

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new GenerateStructuredData()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String[]>write()
                .withDataSourceConfiguration(dc)
                .to("test_example_success_objects")
                .withTableSchema(tableSchema)
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(SnowflakeIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("DROP TABLE IF EXISTS test_example_success_objects;"));
  }

  public static SnowflakeIO.UserDataMapper<String[]> getCsvMapper() {
    return (SnowflakeIO.UserDataMapper<String[]>) recordLine -> recordLine;
  }
}
