package com.polidea.snowflake.test;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.polidea.snowflake.io.SnowflakeIO;
import com.polidea.snowflake.io.SnowflakePipelineOptions;
import com.polidea.snowflake.io.credentials.SnowflakeCredentialsFactory;
import com.polidea.snowflake.io.locations.Location;
import com.polidea.snowflake.io.locations.LocationFactory;
import java.io.File;
import java.sql.SQLException;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Integration tests that checks batch write operation of SnowflakeIO.
 *
 * <p>Example test run: ./gradlew test --tests
 * com.polidea.snowflake.test.BatchWriteTest.writeToInternalWithNamedStageTest
 * -DintegrationTestPipelineOptions='[ "--runner=DataflowRunner", "--project=...",
 * "--stagingLocation=gs://...", "--serverName=...", "--username=...", "--password=...",
 * "--schema=PUBLIC", "--table=...", "--database=...", "--stage=...", "--internalLocation=./test",
 * "--maxNumWorkers=5", "--appName=internal" ]'
 */
public class BatchWriteTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static DataSource dataSource;

  public interface ExampleTestPipelineOptions extends SnowflakePipelineOptions {
    @Description("Table name to connect to.")
    String getTable();

    void setTable(String table);
  }

  static ExampleTestPipelineOptions options;
  static SnowflakeIO.DataSourceConfiguration dc;
  static Location locationSpec;

  @BeforeClass
  public static void setup() throws Exception {
    PipelineOptionsFactory.register(ExampleTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(ExampleTestPipelineOptions.class);

    dc =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withUrl(options.getUrl())
            .withServerName(options.getServerName())
            .withDatabase(options.getDatabase())
            .withWarehouse(options.getWarehouse())
            .withSchema(options.getSchema());

    dataSource = dc.buildDatasource();
    locationSpec = LocationFactory.of(options);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (locationSpec.isInternal()) {
      File directory = new File(options.getInternalLocation());
      FileUtils.deleteDirectory(directory);
    }

    if (!locationSpec.isInternal()) {
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

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));
  }

  // Uses file name template which default is output*
  @Test
  @Ignore
  public void writeToInternalWithNamedStageTest() throws SQLException {
    String query = String.format("CREATE OR REPLACE stage %s;", options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    pipeline
        .apply(GenerateSequence.from(0).to(1000000))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .to(options.getTable())
                .via(locationSpec)
                .withDataSourceConfiguration(dc)
                .withFileNameTemplate("output*")
                .withParallelization(false)
                .withCoder(SerializableCoder.of(String.class)));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  // This is more Beam way test. Parallelization is ON by default
  @Test
  @Ignore
  public void writeToInternalWithNamedStageAndParalleledTest() throws SQLException {
    String query = String.format("CREATE OR REPLACE stage %s;", options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    pipeline
        .apply(GenerateSequence.from(0).to(1000000))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .to(options.getTable())
                .via(locationSpec)
                .withDataSourceConfiguration(dc)
                .withCoder(SerializableCoder.of(String.class)));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToExternalWithStageWithMapperTest() throws SQLException {
    String query =
        String.format(
            "create or replace stage %s \n"
                + "  url = 'gcs://input-test-winter/write/'\n"
                + "  storage_integration = google_integration;",
            options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "External text write IO",
            SnowflakeIO.<String>write()
                .to(options.getTable())
                .via(locationSpec)
                .withDataSourceConfiguration(dc)
                .withCoder(StringUtf8Coder.of()));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToExternalWithIntegrationTest() throws SQLException {

    String query =
        String.format(
            "create or replace stage %s \n"
                + "  url = 'gcs://input-test-winter/write/'\n"
                + "  storage_integration = google_integration;",
            options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    locationSpec =
        LocationFactory.getExternalLocationWithIntegration(
            options.getIntegration(), options.getExternalLocation());

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "External text write IO",
            SnowflakeIO.<String>write()
                .to(options.getTable())
                .via(locationSpec)
                .withDataSourceConfiguration(dc)
                .withCoder(StringUtf8Coder.of()));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToExternalWithIntegrationFailsWithoutStage() {
    locationSpec =
        LocationFactory.getExternalLocationWithIntegration(
            options.getIntegration(), options.getExternalLocation());

    String query = "select t.$1 from @%s t";
    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "External text write IO",
            SnowflakeIO.<Long>write()
                .to(options.getTable())
                .via(locationSpec)
                .withUserDataMapper(getCsvMapper())
                .withQueryTransformation(query)
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.APPEND)
                .withDataSourceConfiguration(dc));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToInternalWithTransformationTest() {
    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());

    String query = "select t.$1 from @%s t";
    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "External text write IO",
            SnowflakeIO.<String>write()
                .to(options.getTable())
                .via(locationSpec)
                .withDataSourceConfiguration(dc)
                .withQueryTransformation(query)
                .withCoder(StringUtf8Coder.of()));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToExternalWithStageKVInput() throws SQLException {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    String query =
        String.format(
            "create or replace stage %s \n"
                + "  url = 'gcs://input-test-winter/write/'\n"
                + "  storage_integration = google_integration;",
            options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    pipeline
        .apply(GenerateSequence.from(0).to(10))
        .apply(ParDo.of(new ParseToKv()))
        .apply(
            "External text write IO",
            SnowflakeIO.<KV<String, Integer>>write()
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(getCsvMapperKV())
                .to(options.getTable())
                .via(locationSpec));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToExternalWithTransformationTest() throws SQLException {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    String prepareQuery =
        String.format(
            "create or replace stage %s \n"
                + "  url = 'gcs://input-test-winter/write/'\n"
                + "  storage_integration = google_integration;",
            options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, prepareQuery);

    String query = "select t.$1 from @%s t";
    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "External text write IO",
            SnowflakeIO.<String>write()
                .to(options.getTable())
                .via(locationSpec)
                .withDataSourceConfiguration(dc)
                .withQueryTransformation(query)
                .withCoder(StringUtf8Coder.of()));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToInternalWithWriteTruncateDispositionSuccess() throws SQLException {
    String query = String.format("CREATE OR REPLACE stage %s;", options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    query = String.format("INSERT INTO %s VALUES ('test')", options.getTable());
    TestUtils.runConnectionWithStatement(dataSource, query);

    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.TRUNCATE)
                .withParallelization(false)
                .withCoder(SerializableCoder.of(String.class)));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToExternalWithWriteTruncateDispositionSuccess() throws SQLException {
    String query = String.format("INSERT INTO %s VALUES ('test')", options.getTable());
    TestUtils.runConnectionWithStatement(dataSource, query);

    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .to(options.getTable())
                .via(locationSpec)
                .withDataSourceConfiguration(dc)
                .withFileNameTemplate("output*")
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.TRUNCATE)
                .withParallelization(false)
                .withCoder(SerializableCoder.of(String.class)));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  @Ignore
  public void writeToInternalWithWriteEmptyDispositionWithNotEmptyTableFails() throws SQLException {
    String query = String.format("INSERT INTO %s VALUES ('test')", options.getTable());
    TestUtils.runConnectionWithStatement(dataSource, query);

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
        "org.apache.beam.sdk.util.UserCodeException: java.lang.RuntimeException: Table is not empty. Aborting COPY with disposition EMPTY");

    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());
    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .to(options.getTable())
                .via(locationSpec)
                .withDataSourceConfiguration(dc)
                .withFileNameTemplate("output*")
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.EMPTY)
                .withParallelization(false)
                .withCoder(SerializableCoder.of(String.class)));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToInternalWithWriteEmptyDispositionWithEmptyTableSuccess() throws SQLException {
    String query = String.format("CREATE OR REPLACE stage %s;", options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));

    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());
    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .to(options.getTable())
                .via(locationSpec)
                .withDataSourceConfiguration(dc)
                .withFileNameTemplate("output*")
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.EMPTY)
                .withParallelization(false)
                .withCoder(SerializableCoder.of(String.class)));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  static SnowflakeIO.UserDataMapper<Long> getCsvMapper() {
    return (SnowflakeIO.UserDataMapper<Long>)
        recordLine -> {
          return recordLine.toString();
        };
  }

  static SnowflakeIO.UserDataMapper<KV<String, Integer>> getCsvMapperKV() {
    return (SnowflakeIO.UserDataMapper<KV<String, Integer>>)
        recordLine -> String.format("%s, %s", recordLine.getKey(), recordLine.getValue());
  }

  static class Parse extends DoFn<Long, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().toString());
    }
  }

  static class ParseToKv extends DoFn<Long, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      KV stringIntKV = KV.of(c.element().toString(), c.element().intValue());
      c.output(stringIntKV);
    }
  }
}
