package net.snowflake.examples;

import java.io.Serializable;
import net.snowflake.io.SnowflakeIO;
import net.snowflake.io.SnowflakePipelineOptions;
import net.snowflake.io.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
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
    String QUERY_OR_TABLE_VALIDATION_GROUP = "QUERY_OR_TABLE_VALIDATION_GROUP";

    @Description("Query to execute.")
    @Validation.Required(groups = QUERY_OR_TABLE_VALIDATION_GROUP)
    String getQuery();

    void setQuery(String query);

    @Description("Table to read.")
    @Validation.Required(groups = QUERY_OR_TABLE_VALIDATION_GROUP)
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

    String query = options.getQuery();
    String externalLocation = options.getExternalLocation();
    String integrationName = options.getStorageIntegration();
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
                .fromQuery(query)
                .withStagingBucketName(externalLocation)
                .withIntegrationName(integrationName)
                .withCsvMapper(
                    new SnowflakeIO.CsvMapper<KV<Integer, String>>() {
                      @Override
                      public KV<Integer, String> mapRow(String[] parts) throws Exception {
                        return KV.of(Integer.valueOf(parts[0]), parts[1]);
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
