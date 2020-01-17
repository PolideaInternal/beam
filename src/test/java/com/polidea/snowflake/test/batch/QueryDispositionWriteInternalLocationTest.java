package com.polidea.snowflake.test.batch;

import static com.polidea.snowflake.test.TestUtils.getCsvMapper;
import static org.junit.Assume.assumeNotNull;

import com.polidea.snowflake.io.SnowflakeIO;
import com.polidea.snowflake.io.credentials.SnowflakeCredentialsFactory;
import com.polidea.snowflake.io.locations.Location;
import com.polidea.snowflake.io.locations.LocationFactory;
import com.polidea.snowflake.test.TestUtils;
import java.io.File;
import java.sql.SQLException;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
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

public class QueryDispositionWriteInternalLocationTest {
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
    assumeNotNull(options.getInternalLocation());
  }

  @After
  public void tearDown() throws Exception {
    if (options.getInternalLocation() != null) {
      File directory = new File(options.getInternalLocation());
      FileUtils.deleteDirectory(directory);
    }

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));
  }

  @Test
  public void writeWithWriteTruncateDispositionSuccess() throws SQLException {
    String query = String.format("CREATE OR REPLACE stage %s;", options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    query = String.format("INSERT INTO %s VALUES ('test')", options.getTable());
    TestUtils.runConnectionWithStatement(dataSource, query);

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Truncate before write",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
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
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.EMPTY)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  public void writeWithWriteEmptyDispositionWithEmptyTableSuccess() throws SQLException {
    String query = String.format("CREATE OR REPLACE stage %s;", options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));

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
