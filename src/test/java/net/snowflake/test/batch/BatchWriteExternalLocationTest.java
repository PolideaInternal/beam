package net.snowflake.test.batch;

import static net.snowflake.test.TestUtils.getCsvMapper;
import static org.junit.Assume.assumeNotNull;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.sql.SQLException;
import javax.sql.DataSource;
import net.snowflake.io.SnowflakeIO;
import net.snowflake.io.credentials.SnowflakeCredentialsFactory;
import net.snowflake.io.locations.Location;
import net.snowflake.io.locations.LocationFactory;
import net.snowflake.test.TestUtils;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.After;
import org.junit.Before;
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
public class BatchWriteExternalLocationTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static DataSource dataSource;

  static BatchTestPipelineOptions options;
  static SnowflakeIO.DataSourceConfiguration dc;
  static Location locationSpec;

  @BeforeClass
  public static void setupAll() {
    PipelineOptionsFactory.register(BatchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BatchTestPipelineOptions.class);

    assumeNotNull(options.getServerName());

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

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));
  }

  @Test
  public void writeToExternalWithStageTest() {
    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(getCsvMapper())
                .to(options.getTable())
                .via(locationSpec));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  public void writeToExternalWithIntegrationTest() {
    locationSpec =
        LocationFactory.getExternalLocationWithIntegration(
            options.getStorageIntegration(), options.getExternalLocation());

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "External text write IO",
            SnowflakeIO.<Long>write()
                .to(options.getTable())
                .via(locationSpec)
                .withUserDataMapper(getCsvMapper())
                .withDataSourceConfiguration(dc));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  public void writeToExternalWithStageWithMapperTest() {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "External text write IO",
            SnowflakeIO.<Long>write()
                .to(options.getTable())
                .via(locationSpec)
                .withDataSourceConfiguration(dc)
                .withUserDataMapper(getCsvMapper()));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  public void writeToExternalWithStageKVInput() {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

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

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  // According to snowflake documentation it is possible to use Query Transformation only with stage
  // (not directly external location and integration)
  // TODO this test is problematic due to issue Outputs for non-root node External text write IO are
  // null
  @Ignore
  @Test
  public void writeToExternalWithIntegrationWithoutStageFails() {
    locationSpec =
        LocationFactory.getExternalLocationWithIntegration(
            options.getStorageIntegration(), options.getExternalLocation());

    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("withQuery() requires stage as location");

    String query = "select t.$1 from %s t";
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

    String query = "select t.$1 from %s t";
    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new ParseToKv()))
        .apply(
            "External text write IO",
            SnowflakeIO.<KV<String, Integer>>write()
                .to(options.getTable())
                .via(locationSpec)
                .withUserDataMapper(getCsvMapperKV())
                .withDataSourceConfiguration(dc)
                .withQueryTransformation(query));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  static class ParseToKv extends DoFn<Long, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      KV stringIntKV = KV.of(c.element().toString(), c.element().intValue());
      c.output(stringIntKV);
    }
  }

  static SnowflakeIO.UserDataMapper<KV<String, Integer>> getCsvMapperKV() {
    return (SnowflakeIO.UserDataMapper<KV<String, Integer>>)
        recordLine -> new String[] {String.valueOf(recordLine.getValue())};
  }
}
