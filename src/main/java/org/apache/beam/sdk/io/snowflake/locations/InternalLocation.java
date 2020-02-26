package org.apache.beam.sdk.io.snowflake.locations;

import java.io.Serializable;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;

public class InternalLocation implements Location, Serializable {
  String stage;
  String filesLocation;

  public InternalLocation(SnowflakePipelineOptions options) {
    this.stage = options.getStage();
    this.filesLocation = options.getInternalLocation();
  }

  public InternalLocation(String stage, String internalLocation) {
    this.stage = stage;
    this.filesLocation = internalLocation;
  }

  @Override
  public String getFilesLocationForCopy() {
    return String.format("@%s", stage);
  }

  @Override
  public Boolean isUsingIntegration() {
    return false;
  }

  @Override
  public String getIntegration() {
    return "";
  }

  @Override
  public Boolean isInternal() {
    return true;
  }

  @Override
  public String getFilesPath() {
    return this.filesLocation;
  }

  @Override
  public String getStage() {
    return stage;
  }
}
