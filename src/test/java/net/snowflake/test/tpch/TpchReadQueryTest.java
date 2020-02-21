package net.snowflake.test.tpch;

import net.snowflake.io.SnowflakeIO;
import net.snowflake.io.credentials.SnowflakeCredentialsFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class TpchReadQueryTest {
  private static final String DATABASE = "SNOWFLAKE_SAMPLE_DATA";
  private static final String QUERY = "SELECT * FROM LINEITEM";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static TpchTestPipelineOptions options;
  private static String stagingBucketName;
  private static String integrationName;
  private static String output;

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(TpchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(TpchTestPipelineOptions.class);
    stagingBucketName = options.getStagingBucketName();
    integrationName = options.getStorageIntegration();
    output = options.getParquetFilesLocation();
    String testSize = options.getTestSize();

    Assume.assumeNotNull(testSize);

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withServerName(options.getServerName())
            .withWarehouse(options.getWarehouse())
            .withDatabase(DATABASE)
            .withSchema(testSize); // selecting tests size is done by selecting schema
  }

  @Test
  public void tpchReadTestForQuery() {
    PCollection<GenericRecord> items =
        pipeline.apply(
            SnowflakeIO.<GenericRecord>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .fromQuery(QUERY)
                .withStagingBucketName(stagingBucketName)
                .withIntegrationName(integrationName)
                .withCsvMapper(TpchTestUtils.getCsvMapper())
                .withCoder(AvroCoder.of(TpchTestUtils.getSchema())));

    items.apply(
        FileIO.<GenericRecord>write().via(ParquetIO.sink(TpchTestUtils.getSchema())).to(output));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
