/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.snowflake.locations;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;

/**
 * Factory class for creating implementations of {@link Location} from {@link
 * SnowflakePipelineOptions}.
 */
public class LocationFactory {

  public static Location of(SnowflakePipelineOptions options) {
    if (options.getStorageIntegration() != null && options.getExternalLocation() != null) {
      return new ExternalIntegrationLocation(options);
    } else if (options.getStage() != null && options.getExternalLocation() != null) {
      return new ExternalStageLocation(options);
    } else if ( options.getStage() != null && options.getInternalLocation() != null) {
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
