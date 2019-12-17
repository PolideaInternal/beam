package com.polidea.snowflake.io;

import java.io.FileInputStream;
import java.io.Serializable;
import java.util.Properties;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
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

@RunWith(JUnit4.class)
public class SnowflakeJDBCIOTest {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeJDBCIOTest.class);
  @Rule public final transient TestPipeline pipelineRead = TestPipeline.create();
  @Rule public final transient TestPipeline pipelineWrite = TestPipeline.create();

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
  static SnowflakeIO.DataSourceConfiguration dcextended;

  public interface SnowflakeTestPipelineOptions
      extends TestPipelineOptions, DataflowPipelineOptions {
    String getAccount();

    void setAccount(String account);

    String getUsername();

    void setUsername(String username);

    String getPassword();

    void setPassword(String password);

    String getOauthToken();

    void setOauthToken(String oauthToken);

    String getPrivateKeyPath();

    void setPrivateKeyPath(String privateKeyPath);

    String getPrivateKeyPassphrase();

    void setPrivateKeyPassphrase(String keyPassphrase);

    String getTempRoot();

    void setTempRoot(String tempRoot);
  }

  @BeforeClass
  public static void setup() throws Exception {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
    options = PipelineOptionsFactory.as(SnowflakeTestPipelineOptions.class);

    Properties props = new Properties();
    props.load(new FileInputStream("src/test/resources/config.properties"));

    if (props.getProperty("runner").equals("Dataflow")) {
      options.setRunner(TestDataflowRunner.class);
      options.setProject(props.getProperty("project"));
      options.setTempLocation(props.getProperty("tempLocation"));
      options.setTempRoot(props.getProperty("tempRoot"));
      output = props.getProperty("output_dataflow");
    } else {
      options.setRunner(DirectRunner.class);
      output = props.getProperty("output_local");
    }

    snowflakeBasicDataSource = new SnowflakeBasicDataSource();

    pass = props.getProperty("pass");
    account = props.getProperty("account");
    user = props.getProperty("user");
    region = props.getProperty("region");
    table = props.getProperty("table");

    serverName = String.format("%s.%s.snowflakecomputing.com/", account, region);
    snowflakeBasicDataSource.setServerName(serverName);

    String url = snowflakeBasicDataSource.getUrl();
    dc =
        SnowflakeIO.DataSourceConfiguration.create("net.snowflake.client.jdbc.SnowflakeDriver", url)
            .withUsername(user)
            .withPassword(pass)
            .withConnectionProperties(
                String.format(
                    "database=%s;schema=%s;",
                    props.getProperty("database"), props.getProperty("schema")));
  }

  static class Parse extends DoFn<TestRow, String> implements Serializable {
    @ProcessElement
    public void processElement(ProcessContext c) {
      System.out.println("Test");
      c.output(c.element().toString());
    }
  }

  @Test
  public void readTest() {
    try {
      PCollection<TestRow> namesAndIds =
          (PCollection<TestRow>)
              pipelineRead.apply(
                  "Read from IO",
                  SnowflakeIO.read()
                      .withDataSourceConfiguration(dc)
                      .withQuery(String.format("SELECT id, name FROM %s LIMIT 1000;", table))
                      .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
                      .withCoder(SerializableCoder.of(TestRow.class)));

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
      PipelineResult pipelineResult = pipelineRead.run(options);
      pipelineResult.waitUntilFinish();
    } catch (RuntimeException e) {
      throw e;
    }
  }

  @Test
  public void writeThenReadTest() {
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
