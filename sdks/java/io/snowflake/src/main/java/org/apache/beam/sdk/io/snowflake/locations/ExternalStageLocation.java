package org.apache.beam.sdk.io.snowflake.locations;

import java.io.Serializable;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;

public class ExternalStageLocation implements Location, Serializable {
  String stage;
  String bucketPath;

  public ExternalStageLocation(SnowflakePipelineOptions options) {
    this.stage = options.getStage();
    this.bucketPath = options.getExternalLocation();
  }

  public ExternalStageLocation(String stage, String bucketPath) {
    this.stage = stage;
    this.bucketPath = bucketPath;
  }

  @Override
  public String getIntegration() {
    return "";
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
  public Boolean isInternal() {
    return false;
  }

  @Override
  public String getFilesPath() {
    return this.bucketPath;
  }

  @Override
  public String getStage() {
    return stage;
  }
}
