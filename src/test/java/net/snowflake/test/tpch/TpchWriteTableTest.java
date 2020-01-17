package net.snowflake.test.tpch;

import net.snowflake.io.SnowflakeIO;
import net.snowflake.io.credentials.SnowflakeCredentialsFactory;
import net.snowflake.io.locations.LocationFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class TpchWriteTableTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static SnowflakeIO.DataSourceConfiguration dataSourceConfiguration;
  private static TpchTestPipelineOptions options;
  private static String parquetFilesLocation;
  private static String table;

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(TpchTestPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(TpchTestPipelineOptions.class);
    parquetFilesLocation = options.getParquetFilesLocation();
    table = options.getTable();

    Assume.assumeNotNull(
        table,
        parquetFilesLocation,
        options.getExternalLocation(),
        options.getStorageIntegration());

    dataSourceConfiguration =
        SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
            .withUrl(options.getUrl())
            .withServerName(options.getServerName())
            .withWarehouse(options.getWarehouse())
            .withDatabase(options.getDatabase())
            .withSchema(options.getSchema());
  }

  @Test
  public void tpchWriteTestForTable() {

    PCollection<GenericRecord> items =
        pipeline.apply(ParquetIO.read(TpchTestUtils.getSchema()).from(parquetFilesLocation));

    items.apply(
        SnowflakeIO.<GenericRecord>write()
            .withDataSourceConfiguration(dataSourceConfiguration)
            .to(table)
            .withWriteDisposition(SnowflakeIO.Write.WriteDisposition.TRUNCATE)
            .via(LocationFactory.of(options))
            .withUserDataMapper(TpchTestUtils.getUserDataMapper()));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
