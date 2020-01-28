package net.snowflake.test.tpch;

import net.snowflake.io.SnowflakeIO;
import net.snowflake.io.credentials.SnowflakeCredentialsFactory;
import net.snowflake.io.data.SFColumn;
import net.snowflake.io.data.SFTableSchema;
import net.snowflake.io.data.numeric.SFNumber;
import net.snowflake.io.data.text.SFVarchar;
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

public class TpchWriteQueryTest {

  private static final String QUERY_TRANSFORMATION = "select t.$1, t.$3, t.$16 from %s t";

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
        table, parquetFilesLocation, options.getExternalLocation(), options.getStage());

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
    SFTableSchema tableSchema = SFTableSchema.of(
            SFColumn.of("L_ORDERKEY", SFNumber.of()),
            SFColumn.of("L_SUPPKEY", SFNumber.of()),
            SFColumn.of("L_COMMENT", SFVarchar.of(44))
    );

    PCollection<GenericRecord> items =
        pipeline.apply(ParquetIO.read(TpchTestUtils.getSchema()).from(parquetFilesLocation));

    items.apply(
        SnowflakeIO.<GenericRecord>write()
            .withDataSourceConfiguration(dataSourceConfiguration)
            .to(table)
            .withQueryTransformation(QUERY_TRANSFORMATION)
            .withCreateDisposition(SnowflakeIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withTableSchema(tableSchema)
            .via(LocationFactory.of(options))
            .withUserDataMapper(TpchTestUtils.getUserDataMapper()));

    PipelineResult pipelineResult = pipeline.run(options);
    pipelineResult.waitUntilFinish();
  }
}
