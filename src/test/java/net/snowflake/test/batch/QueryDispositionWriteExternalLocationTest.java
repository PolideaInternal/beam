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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class QueryDispositionWriteExternalLocationTest {
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
  public void writeWithWriteTruncateDispositionSuccess() throws SQLException {
    String query = String.format("INSERT INTO %s VALUES ('test')", options.getTable());
    TestUtils.runConnectionWithStatement(dataSource, query);

    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Truncate before write",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withUserDataMapper(getCsvMapper())
                .withFileNameTemplate("output*")
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.TRUNCATE)
                .withParallelization(false));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void writeWithWriteEmptyDispositionWithNotEmptyTableFails() throws SQLException {
    String query = String.format("INSERT INTO %s VALUES ('test')", options.getTable());
    TestUtils.runConnectionWithStatement(dataSource, query);

    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
        "java.lang.RuntimeException: Table is not empty. Aborting COPY with disposition EMPTY");

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withUserDataMapper(getCsvMapper())
                .withFileNameTemplate("output*")
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.EMPTY)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  public void writeWithWriteEmptyDispositionWithEmptyTableSuccess() throws SQLException {
    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));

    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.EMPTY)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
