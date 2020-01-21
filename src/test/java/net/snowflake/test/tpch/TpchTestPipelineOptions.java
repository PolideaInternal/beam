package net.snowflake.test.tpch;

import net.snowflake.io.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.Description;

public interface TpchTestPipelineOptions extends SnowflakePipelineOptions {
  @Description("Destination of output data.")
  String getParquetFilesLocation();

  void setParquetFilesLocation(String parquetFilesLocation);

  @Description("Size of test data. TPCH_SF1, TPCH_SF10,  TPCH_SF100,  TPCH_SF1000")
  String getTestSize();

  void setTestSize(String testSize);

  String getTable();

  void setTable(String table);
}
