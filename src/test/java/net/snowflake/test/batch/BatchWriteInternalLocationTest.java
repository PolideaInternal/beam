package net.snowflake.test.batch;

import static net.snowflake.test.TestUtils.getCsvMapper;
import static org.junit.Assume.assumeNotNull;

import java.io.File;
import java.sql.SQLException;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
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
public class BatchWriteInternalLocationTest {
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

  // Uses file name template which default is output*
  @Test
  public void writeWithNamedStageTest() throws SQLException {
    String query = String.format("CREATE OR REPLACE stage %s;", options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

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
                .withParallelization(false));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  // This is more Beam way test. Parallelization is ON by default
  @Test
  public void writeWithNamedStageAndParalleledTest() throws SQLException {
    String query = String.format("CREATE OR REPLACE stage %s;", options.getStage());
    TestUtils.runConnectionWithStatement(dataSource, query);

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withUserDataMapper(getCsvMapper()));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  public void writeWithTransformationTest() {
    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());

    String query = "select t.$1 from %s t";
    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "External text write IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to(options.getTable())
                .via(locationSpec)
                .withUserDataMapper(getCsvMapper())
                .withQueryTransformation(query));
    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
