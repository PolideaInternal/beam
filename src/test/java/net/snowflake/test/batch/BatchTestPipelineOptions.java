package net.snowflake.test.batch;

import net.snowflake.io.SnowflakePipelineOptions;
import org.apache.beam.sdk.options.Description;

public interface BatchTestPipelineOptions extends SnowflakePipelineOptions {
  @Description("Table name to connect to.")
  String getTable();

  void setTable(String table);

  @Description("Stage name to connect to.")
  String getStage();

  void setStage(String stage);

  @Description("External location name to connect to.")
  String getExternalLocation();

  void setExternalLocation(String externalLocation);

  @Description("Internal (local) location name to connect to.")
  String getInternalLocation();

  void setInternalLocation(String internalLocation);
}
