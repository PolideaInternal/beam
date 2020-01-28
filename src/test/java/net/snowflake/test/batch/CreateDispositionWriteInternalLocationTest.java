package net.snowflake.test.batch;

import static net.snowflake.test.TestUtils.getCsvMapper;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.sql.SQLException;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
import net.snowflake.io.SnowflakeIO;
import net.snowflake.io.credentials.SnowflakeCredentialsFactory;
import net.snowflake.io.data.SFColumn;
import net.snowflake.io.data.SFTableSchema;
import net.snowflake.io.data.text.SFVarchar;
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

public class CreateDispositionWriteInternalLocationTest {
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
    assumeNotNull(options.getInternalLocation());
  }

  @After
  public void tearDown() throws Exception {
    if (options.getInternalLocation() != null) {
      File directory = new File(options.getInternalLocation());
      FileUtils.deleteDirectory(directory);
    }
  }

  @Test
  public void writeWithWriteCreateDispositionWithAlreadyCreatedTableSuccess() throws SQLException {
    assumeTrue(options.getInternalLocation() != null);

    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());

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
                .withCreateDisposition(SnowflakeIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));
  }

  @Test
  public void writeWithWriteCreateDispositionWithCreatedTableWithoutSchemaFails() {
    assumeTrue(options.getInternalLocation() != null);

    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
        "java.lang.IllegalArgumentException: The CREATE_IF_NEEDED disposition requires schema if table doesn't exists");

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to("test_example_fail")
                .via(locationSpec)
                .withFileNameTemplate("output*")
                .withUserDataMapper(getCsvMapper())
                .withCreateDisposition(SnowflakeIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }

  @Test
  public void writeWithWriteCreateDispositionWithCreatedTableWithSchemaSuccess()
      throws SQLException {
    assumeTrue(options.getInternalLocation() != null);

    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());

    SFTableSchema tableSchema = new SFTableSchema(SFColumn.of("id", new SFVarchar()));

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Copy IO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to("test_example_success")
                .via(locationSpec)
                .withTableSchema(tableSchema)
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
  public void writeWithWriteCreateDispositionWithCreateNeverSuccess() throws SQLException {
    assumeTrue(options.getInternalLocation() != null);

    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());

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
                .withCreateDisposition(SnowflakeIO.Write.CreateDisposition.CREATE_NEVER)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();

    TestUtils.runConnectionWithStatement(
        dataSource, String.format("TRUNCATE %s;", options.getTable()));
  }

  @Test
  public void writeWithWriteCreateDispositionWithCreateNeededFails() {
    assumeTrue(options.getInternalLocation() != null);

    locationSpec =
        LocationFactory.getInternalLocation(options.getStage(), options.getInternalLocation());

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(
        "net.snowflake.client.jdbc.SnowflakeSQLException: SQL compilation error:"
            + "\nTable 'TEST_FAILURE' does not exist");

    pipeline
        .apply(GenerateSequence.from(0).to(100))
        .apply(
            "Write SnowflakeIO",
            SnowflakeIO.<Long>write()
                .withDataSourceConfiguration(dc)
                .to("test_failure")
                .via(locationSpec)
                .withUserDataMapper(getCsvMapper())
                .withFileNameTemplate("output*")
                .withCreateDisposition(SnowflakeIO.Write.CreateDisposition.CREATE_NEVER)
                .withParallelization(false));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
