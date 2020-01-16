package com.polidea.snowflake.io.locations;

import com.polidea.snowflake.io.SnowflakePipelineOptions;

public class LocationFactory {

  public static Location of(SnowflakePipelineOptions options) {
    if (options.getIntegration() != null && options.getExternalLocation() != null) {
      return new ExternalIntegrationLocation(options);
    } else if (options.getIntegration() == null && options.getExternalLocation() != null) {
      return new ExternalStageLocation(options);
    } else if (options.getInternalLocation() != null) {
      return new InternalLocation(options);
    }
    throw new RuntimeException("Unable to create location");
  }

  public static Location getExternalLocationWithIntegration(String integration, String bucketPath) {
    return new ExternalIntegrationLocation(integration, bucketPath);
  }

  public static Location getExternalLocation(String stage, String bucketPath) {
    return new ExternalStageLocation(stage, bucketPath);
  }

  public static Location getInternalLocation(String stage, String filesPath) {
    return new InternalLocation(stage, filesPath);
  }
}
