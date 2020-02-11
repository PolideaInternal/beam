package net.snowflake.test.batch;

import static org.junit.Assume.assumeNotNull;

import java.sql.SQLException;
import javax.sql.DataSource;
import net.snowflake.io.SnowflakeIO;
import net.snowflake.io.credentials.SnowflakeCredentialsFactory;
import net.snowflake.io.data.SFColumn;
import net.snowflake.io.data.SFTableSchema;
import net.snowflake.io.data.datetime.SFDate;
import net.snowflake.io.data.datetime.SFDateTime;
import net.snowflake.io.data.datetime.SFTime;
import net.snowflake.io.data.datetime.SFTimestamp;
import net.snowflake.io.data.datetime.SFTimestampLTZ;
import net.snowflake.io.data.datetime.SFTimestampNTZ;
import net.snowflake.io.data.datetime.SFTimestampTZ;
import net.snowflake.io.data.structured.SFArray;
import net.snowflake.io.data.structured.SFObject;
import net.snowflake.io.data.structured.SFVariant;
import net.snowflake.io.locations.Location;
import net.snowflake.io.locations.LocationFactory;
import net.snowflake.test.TestUtils;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SchemaDispositionWriteExternal {
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

  static class GenerateDates extends DoFn<Long, String[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String date = "2020-08-25";
      String datetime = "2014-01-01 16:00:00";
      String time = "00:02:03";

      String[] listOfDates = {date, datetime, time, datetime, datetime, datetime, datetime, null};
      c.output(listOfDates);
    }
  }

  static class GenerateStructuredData extends DoFn<Long, String[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String array = "(\"1\", \"2\", \"3\")";
      String json = "{ \"key1\": \"value1\", \"key2\": \"value2\" }";
      String object =
          "{ \"outer_key1\": { \"inner_key1A\": \"1a\", \"inner_key1B\": \"1b\" },  \"outer_key2\": { \"inner_key2\": 2 } } ";

      String[] listOfDates = {json, object, array};
      c.output(listOfDates);
    }
  }

  @Test
  @Ignore
  public void writeToExternalWithCreatedTableWithDatetimeSchemaSuccess() throws SQLException {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    SFTableSchema tableSchema =
        new SFTableSchema(
            SFColumn.of("date", new SFDate()),
            SFColumn.of("datetime", new SFDateTime()),
            SFColumn.of("time", new SFTime()),
            SFColumn.of("timestamp", new SFTimestamp()),
            SFColumn.of("timestamp_ntz", new SFTimestampNTZ()),
            SFColumn.of("timestamp_ltz", new SFTimestampLTZ()),
            SFColumn.of("timestamp_tz", new SFTimestampTZ()));

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
  public void writeToExternalWithCreatedTableWithStructuredDataSchemaSuccess() throws SQLException {
    locationSpec =
        LocationFactory.getExternalLocation(options.getStage(), options.getExternalLocation());

    SFTableSchema tableSchema =
        new SFTableSchema(
            SFColumn.of("variant", new SFVariant()),
            SFColumn.of("object", new SFObject()),
            SFColumn.of("array", new SFArray()));

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

    //    TestUtils.runConnectionWithStatement(
    //        dataSource, String.format("DROP TABLE IF EXISTS test_example_success;"));
  }

  public static SnowflakeIO.UserDataMapper<String[]> getCsvMapper() {
    return (SnowflakeIO.UserDataMapper<String[]>) recordLine -> recordLine;
  }
}
