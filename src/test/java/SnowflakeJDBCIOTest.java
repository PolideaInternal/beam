import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.beam.runners.dataflow.TestDataflowPipelineOptions;
import org.apache.beam.runners.dataflow.TestDataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * To run this tests on Dataflow
 * ./gradlew test -DintegrationTestPipelineOptions='["
 *    --runner=DataflowRunner",
 *    "--project=gcp_project",
 *    "--stagingLocation=gs://gcp_location",
 *    "--account=snowflake_account",
 *    "--username=snowflake_username",
 *    "--table=test_table_name",
 *    "--snowflakeRegion=ex_us-east-1",
 *    "--password=snowflake_password",
 *    "--schema=ex_PUBLIC",
 *    "--output=gs://..."]'
 *
 */
@RunWith(JUnit4.class)
public class SnowflakeJDBCIOTest {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeJDBCIOTest.class);
  @Rule
  public final transient TestPipeline pipelineRead = TestPipeline.create();
  @Rule
  public final transient TestPipeline pipelineWrite = TestPipeline.create();

  static SnowflakeBasicDataSource snowflakeBasicDataSource;
  static SnowflakeIO.DataSourceConfiguration dc;
  static String pass;
  static String user;
  static String serverName;
  static String account;
  static String region;
  static String table;
  static String output;
  static SnowflakeTestPipelineOptions options;

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(SnowflakeTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(SnowflakeTestPipelineOptions.class);

    output = options.getOutput();
    createConnection();
  }

  private static void createConnection(){
    snowflakeBasicDataSource = new SnowflakeBasicDataSource();

    pass = options.getPassword();
    account = options.getAccount();
    user = options.getUsername();
    region = options.getSnowflakeRegion();
    table = options.getTable();

    serverName = String.format("%s.%s.snowflakecomputing.com/", account, region);
    snowflakeBasicDataSource.setServerName(serverName);

    String url = snowflakeBasicDataSource.getUrl();
    dc = SnowflakeIO.DataSourceConfiguration.create(
        "net.snowflake.client.jdbc.SnowflakeDriver",
        url)
        .withUsername(user)
        .withPassword(pass)
        .withConnectionProperties(String.format("database=%s;schema=%s;", options.getDatabase(), options.getSchema()));
  }

  static class Parse extends DoFn<TestRow, String> implements Serializable {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().toString());
    }
  }

  @Test
  public void readTest() {
    try {
      PCollection<TestRow> namesAndIds =
          (PCollection<TestRow>) pipelineRead
              .apply("Read from IO",
                  SnowflakeIO.read()
                      .withDataSourceConfiguration(dc)
                      .withQuery(String.format("SELECT id, name FROM %s LIMIT 1000;", table))
                      .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
                      .withCoder(SerializableCoder.of(TestRow.class))
              );

      PCollection<String> consolidatedHashcode =
          namesAndIds
              .apply("Select rows", ParDo.of(new TestRow.SelectNameFn()))
              .apply("Hash row contents", Combine.globally(new HashingFn()).withoutDefaults());

      PDone printableData =
          namesAndIds
              .apply("Find elements to write", ParDo.of(new Parse()))
              .apply("Write to text file", TextIO.write().to(output));


      PAssert.that(consolidatedHashcode)
          .containsInAnyOrder(TestRow.getExpectedHashForRowCount(1000));
      PipelineResult pipelineResult = pipelineRead.run();
      pipelineResult.waitUntilFinish();
    } catch (
        RuntimeException e) {
      throw e;
    }
  }

  @Test
  public void writeTest() {
    pipelineWrite
        .apply(GenerateSequence.from(0).to(1000))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(
            SnowflakeIO.<TestRow>write()
                .withDataSourceConfiguration(dc)
                .withStatement(String.format("insert into %s values(?, ?)", table))
                .withPreparedStatementSetter(new JdbcTestHelper.PrepareStatementFromTestRow()));

    pipelineWrite.run();
  }
}

