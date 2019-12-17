package com.polidea.snowflake;

import com.polidea.snowflake.io.SnowflakeIO;
import com.polidea.snowflake.io.SnowflakePipelineOptions;
import com.polidea.snowflake.io.credentials.SnowflakeCredentialsFactory;
import java.io.Serializable;
import java.sql.ResultSet;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ReadPipelineExample {

  static class Parse extends DoFn<KV<Integer, String>, String> implements Serializable {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().toString());
    }
  }

  public interface ExamplePipelineOptions extends SnowflakePipelineOptions {

    @Description("Table name to connect to.")
    String getTable();

    void setTable(String table);

    @Description("Destination of output data.")
    String getOutput();

    void setOutput(String output);
  }

  public static void main(String[] args) {
    ExamplePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ExamplePipelineOptions.class);
    Pipeline pipelineRead = Pipeline.create(options);

    String table = options.getTable();
    String output = options.getOutput();

    SnowflakeIO.DataSourceConfiguration dc =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withUrl(options.getUrl())
            .withServerName(options.getServerName())
            .withDatabase(options.getDatabase())
            .withWarehouse(options.getWarehouse())
            .withSchema(options.getSchema());

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

    namesAndIds
        .apply("Find elements to write", ParDo.of(new Parse()))
        .apply("Write to text file", TextIO.write().to(output));

    PipelineResult pipelineResult = pipelineRead.run(options);
    pipelineResult.waitUntilFinish();
  }
}
