package net.snowflake.io.locations;

import java.io.Serializable;
import net.snowflake.io.SnowflakePipelineOptions;

public class ExternalIntegrationLocation implements Location, Serializable {
  String integration;
  String bucketPath;

  public ExternalIntegrationLocation(SnowflakePipelineOptions options) {
    this.integration = options.getStorageIntegration();
    this.bucketPath = options.getExternalLocation();
  }

  public ExternalIntegrationLocation(String integration, String bucketPath) {
    this.integration = integration;
    this.bucketPath = bucketPath;
  }

  @Override
  public String getIntegration() {
    return integration;
  }

  @Override
  public String getFilesLocationForCopy() {
    return String.format("'%s'", bucketPath);
  }

  @Override
  public Boolean isUsingIntegration() {
    return true;
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
    return null;
  }
}
