package com.polidea.snowflake.test;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.polidea.snowflake.io.SnowflakeIO;
import com.polidea.snowflake.io.SnowflakePipelineOptions;
import com.polidea.snowflake.io.credentials.SnowflakeCredentialsFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

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

  public interface ExampleTestPipelineOptions extends SnowflakePipelineOptions {
    @Description("Table name to connect to.")
    String getTable();

    void setTable(String table);

    @Description("Stage name to connect to.")
    String getStage();

    void setStage(String stage);

    @Description("External location name to connect to.")
    String getExternalLocation();

    void setExternalLocation(String externalLocation);

    @Description("Internal (local) location name to connect to.")
    String getInternalLocation();

    void setInternalLocation(String internalLocation);
  }

  static ExampleTestPipelineOptions options;
  static SnowflakeIO.DataSourceConfiguration dc;

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
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (options.getInternalLocation() != null) {
      File directory = new File(options.getInternalLocation());
      FileUtils.deleteDirectory(directory);
    }

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

  // Uses file name template which default is output*
  @Test
  @Ignore
  public void writeToInternalWithNamedStageTest() throws SQLException {
    Connection connection = dc.buildDatasource().getConnection();
    PreparedStatement statement =
        connection.prepareStatement(
            String.format("CREATE OR REPLACE stage %s;", options.getStage()));
    statement.executeQuery();

    pipeline
        .apply(GenerateSequence.from(0).to(1000000))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .withTable(options.getTable())
                .withStage(options.getStage())
                .withInternalLocation(options.getInternalLocation())
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
    Connection connection = dc.buildDatasource().getConnection();
    PreparedStatement statement =
        connection.prepareStatement(
            String.format("CREATE OR REPLACE stage %s;", options.getStage()));
    statement.executeQuery();

    pipeline
        .apply(GenerateSequence.from(0).to(1000000))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .withTable(options.getTable())
                .withStage(options.getStage())
                .withInternalLocation(options.getInternalLocation())
                .withCoder(SerializableCoder.of(String.class)));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToExternalWithStageTest() throws SQLException {
    Connection connection = dc.buildDatasource().getConnection();
    PreparedStatement statement =
        connection.prepareStatement(
            String.format(
                "create or replace stage %s \n"
                    + "  url = 'gcs://input-test-winter/write/'\n"
                    + "  storage_integration = google_integration;",
                options.getStage()));
    statement.executeQuery();

    pipeline
        .apply(GenerateSequence.from(0).to(1000000))
        .apply(ParDo.of(new Parse()))
        .apply(
            "External text write IO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .withTable(options.getTable())
                .withStage(options.getStage())
                .withExternalBucket(options.getExternalLocation())
                .withCoder(StringUtf8Coder.of()));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToInternalWithWriteTruncateDispositionSuccess() {
    Connection connection;
    try {
      connection = dc.buildDatasource().getConnection();
      PreparedStatement statement =
          connection.prepareStatement(
              String.format("CREATE OR REPLACE stage %s;", options.getStage()));
      statement.executeQuery();

      connection = dc.buildDatasource().getConnection();
      PreparedStatement statementInsert =
          connection.prepareStatement(
              String.format("INSERT INTO %s VALUES ('test')", options.getTable()));
      statementInsert.executeQuery();
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .withTable(options.getTable())
                .withStage(options.getStage())
                .withInternalLocation(options.getInternalLocation())
                .withFileNameTemplate("output*")
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.TRUNCATE)
                .withParallelization(false)
                .withCoder(SerializableCoder.of(String.class)));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToInternalWithWriteEmptyDispositionWithNotEmptyTableFails() {
    Connection connection;
    try {
      connection = dc.buildDatasource().getConnection();
      PreparedStatement statement =
          connection.prepareStatement(
              String.format("INSERT INTO %s VALUES ('test')", options.getTable()));
      statement.executeQuery();
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .withTable(options.getTable())
                .withStage(options.getStage())
                .withInternalLocation(options.getInternalLocation())
                .withFileNameTemplate("output*")
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.EMPTY)
                .withParallelization(false)
                .withCoder(SerializableCoder.of(String.class)));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  @Ignore
  public void writeToInternalWithWriteEmptyDispositionWithEmptyTableSuccess() {
    Connection connection;
    try {
      connection = dc.buildDatasource().getConnection();
      PreparedStatement statement =
          connection.prepareStatement(
              String.format("CREATE OR REPLACE stage %s;", options.getStage()));
      statement.executeQuery();

      connection = dc.buildDatasource().getConnection();
      PreparedStatement statementTruncate =
          connection.prepareStatement(String.format("TRUNCATE %s;", options.getTable()));
      statementTruncate.executeQuery();
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(ParDo.of(new Parse()))
        .apply(
            "Copy IO",
            SnowflakeIO.<String>write()
                .withDataSourceConfiguration(dc)
                .withTable(options.getTable())
                .withStage(options.getStage())
                .withInternalLocation(options.getInternalLocation())
                .withFileNameTemplate("output*")
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.EMPTY)
                .withParallelization(false)
                .withCoder(SerializableCoder.of(String.class)));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  public static class Parse extends DoFn<Long, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().toString());
    }
  }
}
