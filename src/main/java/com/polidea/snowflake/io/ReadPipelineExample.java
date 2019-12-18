package com.polidea.snowflake.io;

import java.io.Serializable;
import java.sql.ResultSet;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class ReadPipelineExample {

  static class Parse extends DoFn<KV<Integer, String>, String> implements Serializable {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().toString());
    }
  }

  public interface SnowflakePipelineOptions extends PipelineOptions {
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

    String getTable();

    void setTable(String table);

    String getOutput();

    void setOutput(String output);

    String getDatabase();

    void setDatabase(String database);

    String getSchema();

    void setSchema(String schema);
  }

  public static void main(String[] args) {
    SnowflakePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SnowflakePipelineOptions.class);
    Pipeline pipelineRead = Pipeline.create(options);

    SnowflakeBasicDataSource snowflakeBasicDataSource = new SnowflakeBasicDataSource();

    String pass = options.getPassword();
    String account = options.getAccount();
    String user = options.getUsername();
    String table = options.getTable();

    String output = options.getOutput();

    String serverName = String.format("%s.snowflakecomputing.com/", account);
    snowflakeBasicDataSource.setServerName(serverName);

    String url = snowflakeBasicDataSource.getUrl();
    SnowflakeIO.DataSourceConfiguration dc =
        SnowflakeIO.DataSourceConfiguration.create("net.snowflake.client.jdbc.SnowflakeDriver", url)
            .withUsername(user)
            .withPassword(pass)
            .withConnectionProperties(
                String.format(
                    "database=%s;schema=%s;", options.getDatabase(), options.getSchema()));

    PCollection<KV<Integer, String>> namesAndIds =
        pipelineRead.apply(
            "Read from IO",
            SnowflakeIO.<KV<Integer, String>>read()
                .withDataSourceConfiguration(dc)
                .withQuery(String.format("SELECT id, name FROM %s LIMIT 1000;", table))
                .withRowMapper(
                    new SnowflakeIO.RowMapper<KV<Integer, String>>() {
                      public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
                        return KV.of(resultSet.getInt(1), resultSet.getString(2));
                      }
                    })
                .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));

    PDone printableData =
        namesAndIds
            .apply("Find elements to write", ParDo.of(new Parse()))
            .apply("Write to text file", TextIO.write().to(output));

    PipelineResult pipelineResult = pipelineRead.run(options);
    pipelineResult.waitUntilFinish();
  }
}
